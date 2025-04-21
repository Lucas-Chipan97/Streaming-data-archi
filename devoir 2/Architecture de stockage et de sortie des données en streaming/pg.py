#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from pymongo import MongoClient
import datetime
import os
import psycopg2
from sqlalchemy import create_engine
import time

def migrate_mongo_to_neon():
    """
    Récupère les données de MongoDB et les enregistre dans une base PostgreSQL sur Neon
    """
    mongo_client = None
    try:
        # 1. Connexion à MongoDB (conteneur Docker)
        print("Connexion à MongoDB...")
        mongo_client = MongoClient('localhost', 27018)
        db = mongo_client.ventes_db
        collection = db.ventes
        
        # 2. Récupération des données
        print("Récupération des données MongoDB...")
        cursor = collection.find({})
        data = list(cursor)
        
        if not data:
            print("Aucune donnée trouvée dans la collection MongoDB.")
            return
        
        # 3. Conversion en DataFrame pandas
        df = pd.DataFrame(data)
        
        # 4. Traitement/nettoyage des données
        print("Traitement des données...")
        
        # Conversion de l'ObjectId en string
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)
        
        # Formatage des dates
        if 'date_vente' in df.columns:
            df['date_vente'] = pd.to_datetime(df['date_vente'])
        
        if 'timestamp_traitement' in df.columns:
            df['timestamp_traitement'] = pd.to_datetime(df['timestamp_traitement'])
        
        # 5. Connexion à PostgreSQL sur Neon
        print("Connexion à la base PostgreSQL sur Neon...")
        
        # URL de connexion directe à Neon (remplacez par votre URL)
        # Format typique: postgresql://user:password@endpoint/dbname
        neon_url = "postgresql://neondb_owner:npg_jP8iDg9IfTZR@ep-lingering-star-a4il9uno-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
        
        # Création d'un moteur SQLAlchemy avec l'URL directe
        engine = create_engine(neon_url)
        
        # 6. Écriture des données dans PostgreSQL
        print("Écriture des données dans PostgreSQL...")
        
        # Nom de la table PostgreSQL où enregistrer les données
        table_name = "ventes"
        
        # Écriture du DataFrame dans PostgreSQL
        # if_exists='replace' remplace la table si elle existe déjà
        # Utilisez 'append' pour ajouter aux données existantes
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # 7. Affichage des résultats
        print(f"Migration terminée à {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Nombre de lignes migrées: {len(df)}")
        print(f"Données migrées vers la table PostgreSQL '{table_name}' sur Neon")
        
        # Afficher quelques statistiques de base
        if 'montant' in df.columns:
            print(f"Montant total des ventes: {df['montant'].sum():.2f} €")
            print(f"Montant moyen des ventes: {df['montant'].mean():.2f} €")
        
    except Exception as e:
        print(f"Erreur lors de la migration: {str(e)}")
    finally:
        # Fermeture des connexions
        if mongo_client:
            mongo_client.close()
            print("Connexion MongoDB fermée")

if __name__ == "__main__":
    # Exécuter la migration
    migrate_mongo_to_neon()