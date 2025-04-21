#!/usr/bin/env python3
# kafka_processor.py
# Alternative à flink_processor.py qui ne nécessite pas PyFlink

import json
import time
import logging
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from collections import defaultdict
from datetime import datetime, timedelta

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GracefulShutdown:
    """Gestionnaire pour arrêter proprement le processeur avec Ctrl+C."""
    shutdown_requested = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        logger.info("\nArrêt demandé. Attendez la fin du traitement du message en cours...")
        self.shutdown_requested = True

def create_kafka_consumer(topic, bootstrap_servers, group_id='flink-processor-group'):
    """Crée et retourne une instance de KafkaConsumer."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                max_poll_interval_ms=300000,
                max_poll_records=500
            )
            logger.info(f"Consommateur Kafka créé avec succès. Connecté à {bootstrap_servers}")
            return consumer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"Tentative {retry_count}/{max_retries}: Erreur lors de la création du consommateur Kafka: {e}")
            time.sleep(2 ** retry_count)
    
    logger.error(f"Impossible de créer le consommateur Kafka après {max_retries} tentatives")
    sys.exit(1)

def create_kafka_producer(bootstrap_servers):
    """Crée un producteur Kafka."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',
                retries=5,
                retry_backoff_ms=500,
                request_timeout_ms=30000,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Producteur Kafka créé avec succès. Connecté à {bootstrap_servers}")
            return producer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"Tentative {retry_count}/{max_retries}: Erreur lors de la création du producteur Kafka: {e}")
            time.sleep(2 ** retry_count)
    
    logger.error(f"Impossible de créer le producteur Kafka après {max_retries} tentatives")
    sys.exit(1)

def process_transaction_batch(messages, window_size_minutes=1):
    """
    Traite un lot de transactions et calcule des statistiques par client et par fenêtre de temps.
    Émule le comportement de Flink avec les fenêtres temporelles.
    """
    # Regrouper les messages par client et par fenêtre de temps
    windows = defaultdict(lambda: defaultdict(lambda: {"credit": 0.0, "debit": 0.0}))
    
    for msg in messages:
        # Extraire les valeurs du message
        data = msg.value
        client_id = data.get('client_id')
        transaction_type = data.get('type')
        
        try:
            montant = float(data.get('montant', 0))
        except (ValueError, TypeError):
            logger.warning(f"Montant invalide: {data.get('montant')}")
            montant = 0.0
        
        # Déterminer la fenêtre de temps (arrondir à la minute la plus proche)
        try:
            timestamp = datetime.fromisoformat(data.get('date').replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            # Si la date n'est pas valide, utiliser la date actuelle
            timestamp = datetime.now()
        
        # Arrondir à la fenêtre de temps
        window_start = timestamp.replace(second=0, microsecond=0)
        window_start = window_start.replace(minute=(window_start.minute // window_size_minutes) * window_size_minutes)
        window_key = window_start.isoformat()
        
        # Accumuler les montants par type de transaction
        if transaction_type == 'CREDIT':
            windows[client_id][window_key]['credit'] += montant
        elif transaction_type == 'DEBIT':
            windows[client_id][window_key]['debit'] += montant
    
    # Préparer les résultats aggregés
    results = []
    for client_id, client_windows in windows.items():
        for window_key, amounts in client_windows.items():
            window_start = datetime.fromisoformat(window_key)
            window_end = window_start + timedelta(minutes=window_size_minutes)
            
            result = {
                'client_id': client_id,
                'total_credit': str(amounts['credit']),
                'total_debit': str(amounts['debit']),
                'net_balance': str(amounts['credit'] - amounts['debit']),
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat()
            }
            results.append(result)
    
    return results

def process_transactions(consumer, producer, output_topic, window_size_minutes=1, batch_timeout=5):
    """
    Consomme les transactions, les traite par lots et envoie les résultats agrégés au topic de sortie.
    Émule le comportement de streaming de Flink.
    """
    shutdown = GracefulShutdown()
    batch = []
    last_process_time = time.time()
    
    logger.info(f"Démarrage du traitement des transactions avec fenêtres de {window_size_minutes} minute(s).")
    logger.info(f"Les résultats seront envoyés au topic: {output_topic}")
    logger.info(f"Appuyez sur Ctrl+C pour arrêter.")
    
    try:
        for message in consumer:
            # Ajouter le message au batch
            batch.append(message)
            
            # Traiter le batch si assez de temps s'est écoulé ou si le batch est assez grand
            current_time = time.time()
            if len(batch) >= 100 or (current_time - last_process_time >= batch_timeout):
                if batch:
                    # Traiter le batch
                    logger.info(f"Traitement d'un batch de {len(batch)} transactions...")
                    results = process_transaction_batch(batch, window_size_minutes)
                    
                    # Envoyer les résultats au topic de sortie
                    for result in results:
                        producer.send(output_topic, key=result['client_id'], value=result)
                    
                    producer.flush()
                    logger.info(f"{len(results)} résultats agrégés envoyés à {output_topic}.")
                    
                    # Commiter les offsets
                    consumer.commit()
                    
                    # Réinitialiser le batch
                    batch = []
                    last_process_time = current_time
            
            # Vérifier si un arrêt a été demandé
            if shutdown.shutdown_requested:
                break
    except Exception as e:
        logger.error(f"Erreur lors du traitement des transactions: {e}")
    finally:
        # Traiter les derniers messages si nécessaire
        if batch:
            try:
                logger.info(f"Traitement du dernier batch de {len(batch)} transactions...")
                results = process_transaction_batch(batch, window_size_minutes)
                
                for result in results:
                    producer.send(output_topic, key=result['client_id'], value=result)
                
                producer.flush()
                logger.info(f"{len(results)} résultats agrégés envoyés à {output_topic}.")
                
                consumer.commit()
            except Exception as e:
                logger.error(f"Erreur lors du traitement final: {e}")
        
        # Fermer les connexions
        consumer.close()
        producer.close()
        logger.info("Processeur arrêté.")

def main():
    # Récupérer les arguments
    source_topic = sys.argv[1] if len(sys.argv) > 1 else "transactions-topic"
    sink_topic = sys.argv[2] if len(sys.argv) > 2 else "transactions-aggregated"
    group_id = sys.argv[3] if len(sys.argv) > 3 else "flink-processor-group"
    window_size_minutes = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    
    # Liste des brokers pour la haute disponibilité
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    
    logger.info(f"Configuration du processeur Kafka pour traiter les données de {source_topic} vers {sink_topic}")
    logger.info(f"Taille de la fenêtre: {window_size_minutes} minute(s)")
    
    # Créer le consommateur
    consumer = create_kafka_consumer(source_topic, bootstrap_servers, group_id)
    
    # Créer le producteur
    producer = create_kafka_producer(bootstrap_servers)
    
    # Démarrer le traitement
    process_transactions(consumer, producer, sink_topic, window_size_minutes)

if __name__ == "__main__":
    main()