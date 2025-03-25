#Etape 1
import matplotlib.pyplot as plt
import pymongo as pm
import pandas as pd
import numpy as np
import json

pd.reset_option('display.max_columns')

#Etape 2
client = pm.MongoClient("mongodb://localhost:27017/")
db = client["TP1"]  # Nom de la base de données
collection = db["SuperHeroes"]  # Nom de la collection

print("Connexion réussie à MongoDB !")

#Etape 3
'''with open('SuperHerosComplet.json', 'r', encoding='utf-8') as file:
    data = json.load(file)  # Charger le fichier JSON

result = collection.insert_many(data)
print('Inserted {} documents'.format(len(result.inserted_ids)))

print(collection.find_one())'''

#Etape 4
def index():
    # Création des index
    collection.create_index([("name", 1)])  # Index sur name
    collection.create_index([("powerstats.intelligence", 1)])  # Index sur intelligence
    collection.create_index([("biography.publisher", 1)])  # Index sur publisher

#Etape 5 
def extract_data():
    data = list(collection.find({}, {"_id": 0}))  # Récupération des données sans le champ _id
    df = pd.DataFrame(data)
    print("✅ Données extraites avec succès !")
    return df

#Etape 6

def clean_nested_data(value):
    if isinstance(value, dict):
        return {key: clean_nested_data(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [clean_nested_data(val) for val in value]
    elif value in [None, "-", np.nan]:
        return "Unknown"
    else:
        return value


def clean_data(df):
    """ Étape 6 : Nettoyage et préparation des données """
    
    # Vérifier les valeurs manquantes et les remplacer par "inconnue"
   
    df = df.map(clean_nested_data)
    

    # Normalisation des formats (conversion hauteur en cm et poids en kg)
    # La fonction ajoute 2 nouvelles informations à chaque héros qui s'appellent "height_cm" et "weight_kg" pour ne conserver qu'un système d'unité
    def convert_height(height_list):
        """ Convertit la hauteur en cm """
        try:
            return int(height_list[1].replace(" cm", ""))  # Prend la valeur en cm
        except:
            return "Unknown"

    def convert_weight(weight_list):
        """ Convertit le poids en kg """
        try:
            return int(weight_list[1].replace(" kg", ""))  # Prend la valeur en kg
        except:
            return "Unknown"

    df["height_cm"] = df["appearance"].apply(lambda x: convert_height(x["height"]) if "height" in x else "inconnue")
    df["weight_kg"] = df["appearance"].apply(lambda x: convert_weight(x["weight"]) if "weight" in x else "inconnue")

    # Suppression des colonnes inutiles
    # On peut supprimer l'apparance car les seuls informations utiles sont la taille et le poids
    # Ces derniers sont ajouté en cm et en kg en dohors d'appearance
    df.drop(columns=["slug", "work", "appearance", "connections", "images"], inplace=True, errors="ignore")


    print("✅ Données nettoyées et préparées avec gestion des valeurs manquantes !")
    return df

def show_summary(df):
    """ Étape 6 : Vérification finale - Affichage d'un résumé des données """
    print("\n📊 Aperçu des données :")
    print(df.head())
    #print("\n🔍 Statistiques générales :")
    #print(df.describe(include="all"))


def Etape6():
    df = None 
    df = extract_data()
    df = clean_data(df)
    show_summary(df)
    return df
    


#Etape 7
def menu(df):
    vues(df)
    while True:
        print("\n📌 MENU PRINCIPAL")
        print("1️⃣ - Calcul de statistique")
        print("2️⃣ - Calcul de statistique avec Numpy")
        print("3️⃣ - Histogramme des statiqtiques")
        print("4️⃣ - Graphique du nombre de super héros par éditeur")
        print("5️⃣ - Quitter")
        

        choix = input("👉 Choisissez une option (1-5) : ")

        if choix == "1":
           calculate_statistics(df)
        elif choix == "2":
            calculate_statistics_numpy(df)
        elif choix == "3":
           histogramme_stat(df)
        elif choix == "4":
            graphique_editeur(df)
        elif choix == "5" : 
            print("👋 Au revoir !")
            break
        else:
            print("❌ Option invalide, veuillez réessayer.") 

#Etape 8 
def calculate_statistics(df):   
    # Extraire les powerstats (force, intelligence, vitesse) dans une nouvelle DataFrame
    powerstats = df['powerstats'].apply(pd.Series)

    # Calculer les statistiques descriptives
    print("\nStatistiques descriptives des super-héros :")
    print(f"\nMoyenne de la force : {powerstats['strength'].mean():.2f}")
    print(f"Moyenne de l'intelligence : {powerstats['intelligence'].mean():.2f}")
    print(f"Moyenne de la vitesse : {powerstats['speed'].mean():.2f}")
    
    print(f"\nMédiane de la force : {powerstats['strength'].median():.2f}")
    print(f"Médiane de l'intelligence : {powerstats['intelligence'].median():.2f}")
    print(f"Médiane de la vitesse : {powerstats['speed'].median():.2f}")
    
    print(f"\nVariance de la force : {powerstats['strength'].var():.2f}")
    print(f"Variance de l'intelligence : {powerstats['intelligence'].var():.2f}")
    print(f"Variance de la vitesse : {powerstats['speed'].var():.2f}")


#Etape 9

def calculate_statistics_numpy(df):
    # Extraire les powerstats (force, intelligence, vitesse) dans une nouvelle DataFrame
    powerstats = df['powerstats'].apply(pd.Series)

    # Calculer les statistiques descriptives
    print("\nStatistiques descriptives des super-héros :")
    print(f"\nMoyenne de la force : {np.mean(powerstats['strength']):.2f}")
    print(f"Moyenne de l'intelligence : {np.mean(powerstats['intelligence']):.2f}")
    print(f"Moyenne de la vitesse : {np.mean(powerstats['speed']):.2f}")
    
    print(f"\nMédiane de la force : {np.median(powerstats['strength']):.2f}")
    print(f"Médiane de l'intelligence : {np.median(powerstats['intelligence']):.2f}")
    print(f"Médiane de la vitesse : {np.median(powerstats['speed']):.2f}")
    
    print(f"\nVariance de la force : {np.var(powerstats['strength']):.2f}")
    print(f"Variance de l'intelligence : {np.var(powerstats['intelligence']):.2f}")
    print(f"Variance de la vitesse : {np.var(powerstats['speed']):.2f}")


#Etape 10
def histogramme_stat(df):
    powerstats = df['powerstats'].apply(pd.Series)
    # Histogramme pour l'intelligence
    plt.figure(figsize=(10, 6))
    plt.hist(powerstats['intelligence'], bins=5, color='green', alpha=0.7, label="Intelligence", rwidth=0.5)
    plt.title("Distribution de l'Intelligence des Super-héros")
    plt.xlabel("Intelligence")
    plt.ylabel("Fréquence")
    plt.legend()
    plt.show()

    
    # Histogramme pour la force
    plt.figure(figsize=(10, 6))
    plt.hist(powerstats['strength'], bins=5, color='blue', alpha=0.7, label="Force", rwidth=0.5)
    plt.title("Distribution de la Force des Super-héros")
    plt.xlabel("Force")
    plt.ylabel("Fréquence")
    plt.legend()
    plt.show()

#Etape 11
def extract_publishers(df):
    publishers = []
    for index, row in df.iterrows():
        # Vérifier si 'biography' existe et si 'publisher' existe dans le dictionnaire
        if isinstance(row.get("biography"), dict) and "publisher" in row["biography"]:
            publisher = row["biography"]["publisher"]
            publishers.append(publisher)
        else:
            publishers.append(None)  # Si 'publisher' ou 'biography' n'existe pas, ajouter None
    return publishers

def graphique_editeur(df):
    # Exemple de données
    editeurs = extract_publishers(df)
    editeurs_counts = pd.Series(editeurs).value_counts()
    
    # Graphique à barres pour les éditeurs
    editeurs_counts.plot(kind='bar', color=['blue', 'green'])
    plt.title("Nombre de Super-héros par Editeur")
    plt.xlabel("Editeur")
    plt.ylabel("Nombre de Super-héros")
    plt.xticks(rotation=85)
    plt.show()

#Etape 12 et 13
def vues(df):
    # Extraction des statistiques de puissance sous forme de DataFrame
    powerstats = df['powerstats'].apply(pd.Series)
    
    # Calcul de la moyenne de l'intelligence
    average_intelligence = int(np.mean(powerstats['intelligence']))  # Convertir en entier pour éviter l'erreur

    print(f"Moyenne Intelligence : {average_intelligence}")
    
    
    # Créer une vue pour l'intelligence supérieure à la moyenne
    db.command('create', 'superheros_intelligents', viewOn="SuperHeroes", pipeline=[
        {"$match": {"powerstats.intelligence": {"$gt": average_intelligence}}}
    ])

    # Créer une vue pour l'intelligence supérieure à la moyenne
    db.command('create', 'superheros_forts', viewOn="SuperHeroes", pipeline=[
        {"$sort": {"powerstats.strength": 1}}
    ])


def main():
    index()
    df = Etape6()
    menu(df)
    
    


main()