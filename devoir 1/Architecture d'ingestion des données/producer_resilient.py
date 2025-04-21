#!/usr/bin/env python3
# producer_resilient.py

import csv
import json
import time
import sys
import os
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_topic_if_not_exists(bootstrap_servers, topic_name, num_partitions=6, replication_factor=3):
    """Crée un topic Kafka s'il n'existe pas déjà."""
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    try:
        topic_list = [NewTopic(
            name=topic_name, 
            num_partitions=num_partitions, 
            replication_factor=replication_factor
        )]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Topic {topic_name} créé avec {num_partitions} partitions et facteur de réplication {replication_factor}")
    except TopicAlreadyExistsError:
        logger.info(f"Le topic {topic_name} existe déjà")
    except Exception as e:
        logger.error(f"Erreur lors de la création du topic: {e}")
    finally:
        admin_client.close()

def create_kafka_producer(bootstrap_servers):
    """Crée un producteur Kafka avec des configurations pour la résilience."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',                 # Attend confirmation de tous les replicas
                retries=5,                  # Nombre de retentatives en cas d'échec
                retry_backoff_ms=500,       # Délai entre les tentatives
                request_timeout_ms=30000,   # Timeout pour les requêtes
                max_in_flight_requests_per_connection=1,  # Évite les réordonnancements
                linger_ms=100,              # Batching pour améliorer les performances
                batch_size=16384,           # Taille du batch
                compression_type='snappy'   # Compression des messages
            )
            logger.info(f"Producteur Kafka créé avec succès. Connecté à {bootstrap_servers}")
            return producer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"Tentative {retry_count}/{max_retries}: Erreur lors de la création du producteur Kafka: {e}")
            time.sleep(2 ** retry_count)  # Backoff exponentiel
    
    logger.error(f"Impossible de créer le producteur Kafka après {max_retries} tentatives")
    sys.exit(1)

def send_with_retry(producer, topic, key, value, max_retries=5):
    """Envoie un message à Kafka avec mécanisme de retry."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            future = producer.send(topic, key=key, value=value)
            record_metadata = future.get(timeout=10)
            return record_metadata
        except Exception as e:
            retry_count += 1
            logger.warning(f"Erreur lors de l'envoi: {e}. Tentative {retry_count}/{max_retries}")
            if retry_count >= max_retries:
                logger.error(f"Échec de l'envoi après {max_retries} tentatives")
                return None
            time.sleep(0.5 * (2 ** retry_count))  # Backoff exponentiel
    return None

def continuous_csv_reader(file_path, topic, producer, key_field=None, check_interval=1.0):
    """Lit continuellement un fichier CSV et envoie les nouvelles lignes à Kafka."""
    
    # Attendre que le fichier existe
    while not os.path.exists(file_path):
        logger.info(f"En attente du fichier {file_path}...")
        time.sleep(check_interval)
    
    # Initialiser le suivi de position et les en-têtes
    position = 0
    headers = None
    messages_count = 0
    failed_messages = 0
    
    logger.info(f"Surveillance du fichier {file_path} pour les nouvelles données...")
    logger.info(f"Les données seront envoyées au topic Kafka: {topic}")
    logger.info(f"Appuyez sur Ctrl+C pour arrêter.")
    
    try:
        while True:
            # Vérifier si le fichier existe toujours
            if not os.path.exists(file_path):
                logger.warning(f"Attention: Le fichier {file_path} n'existe plus. En attente de sa création...")
                time.sleep(check_interval)
                continue
                
            # Obtenir la taille actuelle du fichier
            current_size = os.path.getsize(file_path)
            
            # Si le fichier a de nouvelles données
            if current_size > position:
                try:
                    with open(file_path, 'r') as file:
                        # Aller à la dernière position lue
                        file.seek(position)
                        
                        # Lire les nouvelles lignes
                        lines = file.readlines()
                        
                        # Mettre à jour la position pour la prochaine lecture
                        position = file.tell()
                    
                    # Si nous n'avons pas encore les en-têtes, les extraire
                    if headers is None and lines:
                        headers = [h.strip() for h in lines[0].strip().split(',')]
                        lines = lines[1:]  # Exclure la ligne d'en-tête
                        logger.info(f"En-têtes détectés: {headers}")
                        
                    # Traiter chaque nouvelle ligne
                    for line in lines:
                        line = line.strip()
                        if not line:  # Ignorer les lignes vides
                            continue
                            
                        # Parser la ligne CSV
                        values = [v.strip() for v in line.split(',')]
                        
                        # S'assurer que nous avons des en-têtes
                        if headers is None:
                            logger.error("Erreur: Pas d'en-têtes trouvés dans le fichier CSV.")
                            continue
                            
                        # Vérifier que nous avons le bon nombre de valeurs
                        if len(values) != len(headers):
                            logger.warning(f"Ligne ignorée: nombre de valeurs ({len(values)}) différent du nombre d'en-têtes ({len(headers)})")
                            continue
                            
                        # Créer un dictionnaire à partir des en-têtes et valeurs
                        row_dict = dict(zip(headers, values))
                        
                        # Déterminer la clé
                        key = row_dict.get(key_field) if key_field else None
                        
                        # Calculer une partition basée sur la clé (si disponible)
                        # Cela assure que les messages avec la même clé vont toujours dans la même partition
                        
                        # Envoyer à Kafka avec retry
                        record_metadata = send_with_retry(producer, topic, key, row_dict)
                        
                        if record_metadata:
                            messages_count += 1
                            if messages_count % 100 == 0:  # Log toutes les 100 messages pour éviter de spammer
                                logger.info(f"Message #{messages_count} envoyé au topic {record_metadata.topic}, "
                                          f"partition {record_metadata.partition}, "
                                          f"offset {record_metadata.offset}")
                        else:
                            failed_messages += 1
                            logger.error(f"Échec de l'envoi du message: {row_dict}")
                
                except Exception as e:
                    logger.error(f"Erreur lors de la lecture du fichier: {e}")
                    time.sleep(check_interval)
            
            # Attendre avant la prochaine vérification
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        logger.info("\nArrêt du producteur demandé.")
    finally:
        # Assurer que tous les messages sont envoyés avant de quitter
        producer.flush()
        producer.close()
        logger.info(f"\nProducteur arrêté. {messages_count} messages envoyés, {failed_messages} messages en échec au total.")

def main():
    if len(sys.argv) < 3:
        topic = "transactions-topic"
        file_path = "transactions.csv"
        key_field = "id"
        logger.info(f"Utilisation des valeurs par défaut:")
        logger.info(f"  Fichier CSV: {file_path}")
        logger.info(f"  Topic Kafka: {topic}")
        logger.info(f"  Champ clé: {key_field}")
    else:
        # Récupérer les arguments
        file_path = sys.argv[1]
        topic = sys.argv[2]
        key_field = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Liste des brokers pour la haute disponibilité
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    
    # Créer le topic avec la bonne configuration si nécessaire
    create_topic_if_not_exists(bootstrap_servers, topic)
    
    # Créer le producteur Kafka
    producer = create_kafka_producer(bootstrap_servers)
    
    # Démarrer la surveillance et l'envoi en continu
    continuous_csv_reader(file_path, topic, producer, key_field)

if __name__ == "__main__":
    main()