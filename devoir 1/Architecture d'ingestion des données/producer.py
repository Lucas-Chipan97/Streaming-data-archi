#!/usr/bin/env python3
#producer.py

import csv
import json
import time
import sys
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_kafka_producer():
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        return producer
    except KafkaError as e:
        print(f"Erreur lors de la création du producteur Kafka: {e}")
        sys.exit(1)

def continuous_csv_reader(file_path, topic, producer, key_field=None, check_interval=1.0):
    
    # Attendre que le fichier existe
    while not os.path.exists(file_path):
        print(f"En attente du fichier {file_path}...")
        time.sleep(check_interval)
    
    # Initiliser le suivi de position et les en-têtes
    position = 0
    headers = None
    messages_count = 0
    
    print(f"Surveillance du fichier {file_path} pour les nouvelles données...")
    print(f"Les données seront envoyées au topic Kafka: {topic}")
    print(f"Appuyez sur Ctrl+C pour arrêter.")
    
    try:
        while True:
            # Vérifier si le fichier existe toujours
            if not os.path.exists(file_path):
                print(f"Attention: Le fichier {file_path} n'existe plus. En attente de sa création...")
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
                        headers = lines[0].strip().split(',')
                        lines = lines[1:]  # Exclure la ligne d'en-tête
                        
                    # Traiter chaque nouvelle ligne
                    for line in lines:
                        line = line.strip()
                        if not line:  # Ignorer les lignes vides
                            continue
                            
                        # Parser la ligne CSV
                        values = line.split(',')
                        
                        # S'assurer que nous avons des en-têtes
                        if headers is None:
                            print("Erreur: Pas d'en-têtes trouvés dans le fichier CSV.")
                            continue
                            
                        # Créer un dictionnaire à partir des en-têtes et valeurs
                        row_dict = dict(zip(headers, values))
                        
                        # Déterminer la clé
                        key = row_dict.get(key_field) if key_field else None
                        
                        # Envoyer à Kafka
                        try:
                            future = producer.send(topic, key=key, value=row_dict)
                            record_metadata = future.get(timeout=10)
                            
                            messages_count += 1
                            
                            print(f"Message #{messages_count} envoyé au topic {record_metadata.topic}, "
                                  f"partition {record_metadata.partition}, "
                                  f"offset {record_metadata.offset}")
                            
                        except Exception as e:
                            print(f"Erreur lors de l'envoi des données: {e}")
                
                except Exception as e:
                    print(f"Erreur lors de la lecture du fichier: {e}")
                    time.sleep(check_interval)
            
            # Attendre avant la prochaine vérification
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        print("\nArrêt du producteur demandé.")
    finally:
        # Assurer que tous les messages sont envoyés avant de quitter
        producer.flush()
        producer.close()
        print(f"\nProducteur arrêté. {messages_count} messages envoyés au total.")

def main():
    if len(sys.argv) < 3:
        topic = "transactions-topic"
        file_path = "transactions.csv"
        key_field = "id"
        print(f"Utilisation des valeurs par défaut:")
        print(f"  Fichier CSV: {file_path}")
        print(f"  Topic Kafka: {topic}")
        print(f"  Champ clé: {key_field}")
    else:
        # Récupérer les arguments
        file_path = sys.argv[1]
        topic = sys.argv[2]
        key_field = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Créer le producteur Kafka
    producer = create_kafka_producer()
    
    # Démarrer la surveillance et l'envoi en continu
    continuous_csv_reader(file_path, topic, producer, key_field)

if __name__ == "__main__":
    main()