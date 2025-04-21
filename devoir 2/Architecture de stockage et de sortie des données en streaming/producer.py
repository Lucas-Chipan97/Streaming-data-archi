#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration du producteur Kafka (ports modifiés)
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9093'  # Port modifié
KAFKA_TOPIC = 'ventes-stream'

# Liste des produits fictifs
PRODUITS = [
    "Téléphone intelligent", "Ordinateur portable", "Tablette", "Écouteurs sans fil", 
    "Montre connectée", "Enceinte Bluetooth", "Téléviseur", "Console de jeux", 
    "Appareil photo", "Drone"
]

# Liste des régions de vente
REGIONS = [
    "France", "Allemagne", "Royaume-Uni", "Espagne", "Italie", 
    "Belgique", "Pays-Bas", "Suisse", "Portugal", "Autriche"
]

def create_producer():
    """Crée et retourne une instance du producteur Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("Producteur Kafka connecté à %s", KAFKA_BOOTSTRAP_SERVERS)
        return producer
    except Exception as e:
        logger.error("Erreur lors de la création du producteur Kafka: %s", str(e))
        raise

def generate_sale():
    """Génère une vente aléatoire"""
    # Date de vente (dans les dernières 24 heures)
    now = datetime.now()
    random_minutes = random.randint(0, 1440)  # Jusqu'à 24 heures en arrière
    sale_datetime = now - timedelta(minutes=random_minutes)
    
    # Sélection aléatoire d'un produit et d'une région
    produit = random.choice(PRODUITS)
    region = random.choice(REGIONS)
    
    # Montant de la vente (entre 50 et 2000 euros)
    montant = round(random.uniform(50, 2000), 2)
    
    return {
        "date_vente": sale_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "montant": montant,
        "produit": produit,
        "region": region
    }

def run_producer():
    """Fonction principale du producteur"""
    producer = create_producer()
    
    try:
        while True:
            # Générer une ou plusieurs ventes
            num_sales = random.randint(1, 5)  # Générer entre 1 et 5 ventes à la fois
            
            for _ in range(num_sales):
                sale_data = generate_sale()
                
                # Envoyer la vente au topic Kafka
                producer.send(KAFKA_TOPIC, value=sale_data)
                logger.info(f"Vente envoyée: {sale_data}")
            
            # Envoyer tous les messages en attente
            producer.flush()
            
            # Attendre un délai aléatoire avant la prochaine génération
            # (entre 1 et 5 secondes)
            time.sleep(random.uniform(1, 5))
    
    except KeyboardInterrupt:
        logger.info("Arrêt du producteur demandé par l'utilisateur")
    except Exception as e:
        logger.error("Erreur lors de l'exécution du producteur: %s", str(e))
    finally:
        # Fermer proprement le producteur
        if producer:
            producer.close()
            logger.info("Producteur fermé")

if __name__ == "__main__":
    logger.info("Démarrage du producteur de données de ventes")
    run_producer()