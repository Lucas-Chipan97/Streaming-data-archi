#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
import datetime
import time
import os
import schedule

def export_to_csv():
    """
    Exporte les données de la collection MongoDB vers un fichier CSV
    """
    try:
        # Connexion à MongoDB (conteneur Docker)
        client = MongoClient('localhost', 27018)
        db = client.ventes_db
        collection = db.ventes
        
        # Récupération des données
        cursor = collection.find({})
        data = list(cursor)
        
        if not data:
            print("Aucune donnée trouvée dans la collection.")
            return
        
        # Conversion en DataFrame pandas
        df = pd.DataFrame(data)
        
        # Conversion de l'ObjectId en string pour éviter les erreurs de sérialisation
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)
        
        # Formatage de la date pour qu'elle soit lisible par Tableau
        if 'date_vente' in df.columns:
            df['date_vente'] = df['date_vente'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x
            )
        
        if 'timestamp_traitement' in df.columns:
            df['timestamp_traitement'] = df['timestamp_traitement'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x
            )
        
        # Export vers CSV
        csv_filename = 'ventes_data.csv'
        df.to_csv(csv_filename, index=False)
        
        # Afficher des informations sur les données exportées
        print(f"Données exportées vers {csv_filename} à {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Nombre de lignes exportées: {len(df)}")
        
    except Exception as e:
        print(f"Erreur lors de l'export: {str(e)}")
    finally:
        # Fermeture de la connexion
        if 'client' in locals():
            client.close()

# Exécuter l'export immédiatement
export_to_csv()

# Planifier l'export toutes les 2 minutes
schedule.every(2).minutes.do(export_to_csv)

print("Export programmé toutes les 2 minutes. Appuyez sur Ctrl+C pour arrêter.")

# Boucle principale
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Export programmé arrêté par l'utilisateur.")