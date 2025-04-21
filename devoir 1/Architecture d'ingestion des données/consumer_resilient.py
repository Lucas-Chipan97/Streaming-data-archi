#!/usr/bin/env python3
# consumer_resilient.py

import json
import sys
import signal
import logging
import time
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GracefulShutdown:
    """Gestionnaire pour arrêter proprement le consommateur avec Ctrl+C."""
    shutdown_requested = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        logger.info("\nArrêt demandé. Attendez la fin du traitement du message en cours...")
        self.shutdown_requested = True

def create_kafka_consumer(topic, bootstrap_servers, group_id='my-consumer-group', auto_offset_reset='earliest'):
    """Crée et retourne une instance de KafkaConsumer avec des configurations pour la résilience."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False,  # Gestion manuelle des offsets pour fiabilité
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                max_poll_interval_ms=300000,  # 5 minutes
                max_poll_records=500,         # Nombre de records maximum par poll
                session_timeout_ms=30000,     # Timeout de session
                heartbeat_interval_ms=10000,  # Intervalle de heartbeat
                fetch_max_wait_ms=500,        # Attente maximale pour un fetch
                fetch_min_bytes=1,            # Fetch même avec peu de données
                fetch_max_bytes=52428800,     # 50 Mo maximum par fetch
                request_timeout_ms=40000      # Timeout pour les requêtes
            )
            logger.info(f"Consommateur Kafka créé avec succès. Connecté à {bootstrap_servers}")
            return consumer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"Tentative {retry_count}/{max_retries}: Erreur lors de la création du consommateur Kafka: {e}")
            time.sleep(2 ** retry_count)  # Backoff exponentiel
    
    logger.error(f"Impossible de créer le consommateur Kafka après {max_retries} tentatives")
    sys.exit(1)

def process_message(msg):
    """Traite un message Kafka."""
    # Extraire les informations du message
    key = msg.key
    value = msg.value
    topic = msg.topic
    partition = msg.partition
    offset = msg.offset
    
    # Afficher les informations
    logger.info(f"\n{'='*60}")
    logger.info(f"Réception d'un message:")
    logger.info(f"Topic    : {topic}")
    logger.info(f"Partition: {partition}")
    logger.info(f"Offset   : {offset}")
    logger.info(f"Clé      : {key}")
    logger.info(f"Valeur   : {json.dumps(value, indent=2, ensure_ascii=False)}")
    logger.info(f"{'='*60}")
    
    # Ici, vous pouvez ajouter votre logique métier
    # Par exemple, transformations, enrichissements, stockage, etc.
    
    # Simuler un traitement
    processing_time = 0.1  # 100ms
    time.sleep(processing_time)
    
    return True

def consume_messages(consumer, handler=None, max_messages=None, commit_interval=100):
    """Consomme les messages de Kafka avec gestion des erreurs et commits périodiques."""
    
    if handler is None:
        handler = process_message
    
    # Configuration pour l'arrêt gracieux
    shutdown = GracefulShutdown()
    
    logger.info(f"Démarrage du consommateur. En attente de messages...")
    logger.info(f"Appuyez sur Ctrl+C pour arrêter.")
    
    count = 0
    last_commit = 0
    
    try:
        for message in consumer:
            try:
                # Traiter le message
                success = handler(message)
                
                if success:
                    count += 1
                    
                    # Commit des offsets à intervalles réguliers
                    if count % commit_interval == 0:
                        consumer.commit()
                        last_commit = count
                        logger.info(f"Offset commité après {count} messages traités.")
            except Exception as e:
                logger.error(f"Erreur lors du traitement du message: {e}")
                # Option: implémentez une logique de gestion des erreurs ici
                # Par exemple, envoyer à un topic d'erreurs, ou retenter
            
            # Vérifier si nous devons arrêter
            if shutdown.shutdown_requested or (max_messages and count >= max_messages):
                break
    except Exception as e:
        logger.error(f"Erreur lors de la consommation des messages: {e}")
    finally:
        # Commit des derniers messages traités si nécessaire
        if count > last_commit:
            try:
                consumer.commit()
                logger.info(f"Offsets finaux commités.")
            except Exception as e:
                logger.error(f"Erreur lors du commit final: {e}")
        
        # Fermer proprement le consommateur
        try:
            consumer.close()
            logger.info(f"\nConsommateur arrêté. {count} messages traités au total.")
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture du consommateur: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python consumer_resilient.py <topic_kafka> [groupe_id] [max_messages]")
        sys.exit(1)
    
    # Récupérer les arguments
    topic = sys.argv[1]
    group_id = sys.argv[2] if len(sys.argv) > 2 else 'my-consumer-group'
    max_messages = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    logger.info(f"Connexion à Kafka et écoute du topic: {topic}")
    logger.info(f"Groupe ID: {group_id}")
    if max_messages:
        logger.info(f"Arrêt automatique après {max_messages} messages.")
    
    # Liste des brokers pour la haute disponibilité
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    
    # Créer le consommateur
    consumer = create_kafka_consumer(topic, bootstrap_servers, group_id)
    
    # Consommer les messages
    consume_messages(consumer, max_messages=max_messages)

if __name__ == "__main__":
    main()