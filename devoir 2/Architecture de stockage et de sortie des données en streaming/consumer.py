#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration Kafka (ports modifiés)
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9093'  # Port modifié
KAFKA_TOPIC = 'ventes-stream'
KAFKA_GROUP_ID = 'ventes-consumer-group'

# Configuration MongoDB (port modifié)
MONGO_URI = 'mongodb://mongodb:27017/'  # Le port reste 27017 en interne
MONGO_DB = 'ventes_db'
MONGO_COLLECTION = 'ventes'

def create_consumer():
    """Crée et retourne une instance du consommateur Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False
        )
        logger.info("Consommateur Kafka connecté à %s", KAFKA_BOOTSTRAP_SERVERS)
        return consumer
    except Exception as e:
        logger.error("Erreur lors de la création du consommateur Kafka: %s", str(e))
        raise

def connect_to_mongodb():
    """Établit une connexion à MongoDB et retourne le client et la collection"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Création d'index pour améliorer les performances des requêtes
        collection.create_index([("date_vente", 1)])
        collection.create_index([("produit", 1)])
        collection.create_index([("region", 1)])
        
        logger.info("Connexion établie avec MongoDB à %s", MONGO_URI)
        return client, collection
    except Exception as e:
        logger.error("Erreur lors de la connexion à MongoDB: %s", str(e))
        raise

def process_message(message, collection):
    """Traite et stocke un message dans MongoDB"""
    try:
        # Récupérer les données du message
        sale_data = message.value
        
        # Convertir la date de string en objet datetime pour MongoDB
        if 'date_vente' in sale_data:
            sale_data['date_vente'] = datetime.strptime(sale_data['date_vente'], "%Y-%m-%d %H:%M:%S")
        
        # Ajouter un timestamp de traitement
        sale_data['timestamp_traitement'] = datetime.now()
        
        # Insérer dans MongoDB
        result = collection.insert_one(sale_data)
        logger.info(f"Vente enregistrée dans MongoDB (ID: {result.inserted_id}): {sale_data}")
        
        return True
    except Exception as e:
        logger.error("Erreur lors du traitement du message: %s", str(e))
        return False

def run_consumer():
    """Fonction principale du consommateur"""
    consumer = create_consumer()
    mongo_client, collection = connect_to_mongodb()
    
    try:
        logger.info("Démarrage de la consommation des messages...")
        for message in consumer:
            success = process_message(message, collection)
            
            if success:
                # Valider manuellement l'offset après traitement réussi
                consumer.commit()
            
    except KeyboardInterrupt:
        logger.info("Arrêt du consommateur demandé par l'utilisateur")
    except Exception as e:
        logger.error("Erreur lors de l'exécution du consommateur: %s", str(e))
    finally:
        # Fermer proprement les connexions
        if consumer:
            consumer.close()
            logger.info("Consommateur fermé")
        
        if mongo_client:
            mongo_client.close()
            logger.info("Connexion MongoDB fermée")

if __name__ == "__main__":
    logger.info("Démarrage du consommateur de données de ventes")
    
    # Attendre que Kafka et MongoDB soient prêts
    time.sleep(10)
    
    run_consumer()