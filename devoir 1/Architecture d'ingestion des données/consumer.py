#!/usr/bin/env python3
# consumer.py

import json
import sys
import signal
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class GracefulShutdown:
    """Gestionnaire pour arrêter proprement le consommateur avec Ctrl+C."""
    shutdown_requested = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        print("\nArrêt demandé. Attendez la fin du traitement du message en cours...")
        self.shutdown_requested = True

def create_kafka_consumer(topic, group_id='my-consumer-group', auto_offset_reset='earliest'):
    """Crée et retourne une instance de KafkaConsumer."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        return consumer
    except KafkaError as e:
        print(f"Erreur lors de la création du consommateur Kafka: {e}")
        sys.exit(1)

def process_message(msg):
    
    # Extraire les informations du message
    key = msg.key
    value = msg.value
    topic = msg.topic
    partition = msg.partition
    offset = msg.offset
    
    # Afficher les informations
    print(f"\n{'='*60}")
    print(f"Réception d'un message:")
    print(f"Topic    : {topic}")
    print(f"Partition: {partition}")
    print(f"Offset   : {offset}")
    print(f"Clé      : {key}")
    print(f"Valeur   : {json.dumps(value, indent=2, ensure_ascii=False)}")
    print(f"{'='*60}")
    
    # Ici, vous pouvez ajouter votre logique métier
    # Par exemple, transformations, enrichissements, stockage, etc.
    
    return True

def consume_messages(consumer, handler=None, max_messages=None):
    
    if handler is None:
        handler = process_message
    
    # Configuration pour l'arrêt gracieux
    shutdown = GracefulShutdown()
    
    print(f"Démarrage du consommateur. En attente de messages...")
    print(f"Appuyez sur Ctrl+C pour arrêter.")
    
    count = 0
    try:
        for message in consumer:
            # Traiter le message
            success = handler(message)
            
            if success:
                count += 1
                print(f"Message {count} traité avec succès.")
            
            # Vérifier si nous devons arrêter
            if shutdown.shutdown_requested or (max_messages and count >= max_messages):
                break
    except Exception as e:
        print(f"Erreur lors de la consommation des messages: {e}")
    finally:
        # Fermer proprement le consommateur
        consumer.close()
        print(f"\nConsommateur arrêté. {count} messages traités au total.")

def main():
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <topic_kafka> [groupe_id] [max_messages]")
        sys.exit(1)
    
    # Récupérer les arguments
    topic = sys.argv[1]
    group_id = sys.argv[2] if len(sys.argv) > 2 else 'my-consumer-group'
    max_messages = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    print(f"Connexion à Kafka et écoute du topic: {topic}")
    print(f"Groupe ID: {group_id}")
    if max_messages:
        print(f"Arrêt automatique après {max_messages} messages.")
    
    # Créer le consommateur
    consumer = create_kafka_consumer(topic, group_id)
    
    # Consommer les messages
    consume_messages(consumer, max_messages=max_messages)

if __name__ == "__main__":
    main()