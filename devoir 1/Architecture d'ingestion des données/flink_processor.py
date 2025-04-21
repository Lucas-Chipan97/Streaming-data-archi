#!/usr/bin/env python3
# flink_processor.py

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema,  Rowtime
import time
import logging
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_kafka_source_table(t_env, kafka_brokers, topic, group_id):
    t_env.execute_sql(f"""
    CREATE TABLE source_table (
        id STRING,
        `date` STRING, -- Utiliser des guillemets backticks autour de date
        client_id STRING,
        montant DOUBLE,
        type STRING,
        event_time AS TO_TIMESTAMP(`date`),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{kafka_brokers}',
        'properties.group.id' = '{group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)

def create_kafka_sink_table(t_env, kafka_brokers, topic):
    t_env.execute_sql(f"""
    CREATE TABLE sink_table (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        transaction_count BIGINT,
        total_amount DOUBLE,
        avg_amount DOUBLE,
        min_amount DOUBLE,
        max_amount DOUBLE,
        type STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{kafka_brokers}',
        'format' = 'json'
    )
    """)

def process_transactions(t_env):
    return t_env.sql_query("""
        SELECT 
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS transaction_count,
            SUM(montant) AS total_amount,
            AVG(montant) AS avg_amount,
            MIN(montant) AS min_amount,
            MAX(montant) AS max_amount,
            type
        FROM source_table
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' MINUTE),
            type
    """)


def process_transactions(t_env):
    return t_env.sql_query("""
        /*+ OPTIONS('parallelism' = '8') */
        SELECT 
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS transaction_count,
            SUM(montant) AS total_amount,
            AVG(montant) AS avg_amount,
            MIN(montant) AS min_amount,
            MAX(montant) AS max_amount,
            type
        FROM source_table
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' MINUTE),
            type
    """)

def main():
    # Configurez l'environnement
    env = StreamExecutionEnvironment.get_execution_environment()
    

    # Définir le parallélisme global
    env.set_parallelism(4)
    
    # Configurez l'environnement de table
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, settings)

    # Configurez l'environnement de table
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # Définir les paramètres
    kafka_brokers = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
    source_topic = "transactions-topic"
    sink_topic = "transactions-aggregated"
    group_id = "flink-processor-group"
    
    # Créer les tables source et sink
    create_kafka_source_table(t_env, kafka_brokers, source_topic, group_id)
    create_kafka_sink_table(t_env, kafka_brokers, sink_topic)
    
    # Exécuter la requête d'agrégation et l'insérer dans la table sink
    statement_set = t_env.create_statement_set()
    
    # Ajouter l'insertion à partir de la requête d'agrégation
    aggregation_query = process_transactions(t_env)
    statement_set.add_insert("sink_table", aggregation_query)
    
    # Exécuter le job
    statement_set.execute()
    
    logger.info(f"Job soumis avec succès pour traitement des transactions de {source_topic} vers {sink_topic}")
if __name__ == "__main__":
    kafka_brokers = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
    main()