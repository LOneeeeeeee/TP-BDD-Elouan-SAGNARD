import boto3
from botocore.client import Config
from botocore.exceptions import *
import uuid
from datetime import datetime, timedelta
from collections import Counter


# Configure AWS credentials (dummy values in this case)
boto3.setup_default_session(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key=' wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE',
    region_name='us-west-2'
)

def create_dynamodb_resource():
    """Crée une ressource DynamoDB connectée à l'instance locale."""
    return boto3.resource('dynamodb', endpoint_url='http://localhost:8000')



def create_livres_table(dynamodb):
    try:
        dynamodb.create_table(
            TableName='livres',
            KeySchema=[{'AttributeName': 'ISBN', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'ISBN', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print("Table 'livres' créée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création de la table 'livres': {e}")

def create_emprunts_table(dynamodb):
    try:
        dynamodb.create_table(
            TableName='emprunts',
            KeySchema=[{'AttributeName': 'emprunt_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'emprunt_id', 'AttributeType': 'S'},
                {'AttributeName': 'ISBN', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'ISBNIndex',
                    'KeySchema': [{'AttributeName': 'ISBN', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print("Table 'emprunts' créée avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création de la table 'emprunts': {e}")

def check_table_exists(dynamodb, table_name):
    # Utiliser le client DynamoDB pour lister les tables
    client = dynamodb.meta.client
    
    # Initialisation pour la pagination
    paginator = client.get_paginator('list_tables')
    page_iterator = paginator.paginate()
    
    # Parcourir toutes les tables pour voir si table_name existe
    for page in page_iterator:
        if table_name in page['TableNames']:
            return True
    return False

'''

# Ajouter un livre
def ajouter_livre(dynamodb, table_livres, isbn, titre, auteur, annee):
    try:
        table = dynamodb.Table(table_livres)
        table.put_item(
            Item={
                'ISBN': isbn,
                'titre': titre,
                'auteur': auteur,
                'annee_publication': annee,
                'disponible': True
            }
        )
        print(f"Livre '{titre}' ajouté avec succès !")
    except Exception as e:
        print(f"Erreur lors de l'ajout : {e}")

# Récupérer un livre par ISBN
def recuperer_livre(dynamodb, table_livres, isbn):
    try:
        table = dynamodb.Table(table_livres)
        response = table.get_item(Key={'ISBN': isbn})
        if 'Item' in response:
            return response['Item']
        else:
            print("Livre non trouvé.")
            return None
    except Exception as e:
        print(f"Erreur lors de la récupération : {e}")

# Lister tous les livres
def lister_livres(dynamodb, table_livres):
    try:
        table = dynamodb.Table(table_livres)
        response = table.scan()
        return response.get('Items', [])
    except Exception as e:
        print(f"Erreur lors de la récupération des livres : {e}")

# Mettre à jour la disponibilité d’un livre
def mettre_a_jour_disponibilite(dynamodb, table_livres, isbn, disponible):
    try:
        table = dynamodb.Table(table_livres)
        table.update_item(
            Key={'ISBN': isbn},
            UpdateExpression="SET disponible = :d",
            ExpressionAttributeValues={':d': disponible}
        )
        print(f"Disponibilité mise à jour pour ISBN {isbn}.")
    except Exception as e:
        print(f"Erreur lors de la mise à jour : {e}")

# Supprimer un livre
def supprimer_livre(dynamodb, table_livres, isbn):
    try:
        table = dynamodb.Table(table_livres)
        table.delete_item(Key={'ISBN': isbn})
        print(f"Livre ISBN {isbn} supprimé.")
    except Exception as e:
        print(f"Erreur lors de la suppression : {e}")

# Ajouter un emprunt
def emprunter_livre(dynamodb, table_livres, table_emprunts, isbn, utilisateur):
    table_livres = dynamodb.Table(table_livres)
    table_emprunts = dynamodb.Table(table_emprunts)

    # Vérifier si le livre est disponible
    livre = table_livres.get_item(Key={'ISBN': isbn}).get('Item')

    if not livre:
        print("Livre non trouvé.")
        return
    if not livre['disponible']:
        print("Ce livre est déjà emprunté.")
        return

    # Générer un ID unique pour l'emprunt
    emprunt_id = str(uuid.uuid4())
    date_emprunt = datetime.now().strftime("%Y-%m-%d")

    try:
        # Ajouter l'emprunt
        table_emprunts.put_item(
            Item={
                'emprunt_id': emprunt_id,
                'ISBN': isbn,
                'utilisateur': utilisateur,
                'date_emprunt': date_emprunt,
                'date_retour': None
            }
        )
        # Mettre à jour la disponibilité du livre
        table_livres.update_item(
            Key={'ISBN': isbn},
            UpdateExpression="SET disponible = :d",
            ExpressionAttributeValues={':d': False}
        )
        print(f"Livre {isbn} emprunté par {utilisateur} avec succès !")
    except Exception as e:
        print(f"Erreur lors de l'emprunt : {e}")

# Récupérer un emprunt par ID
def recuperer_emprunt(dynamodb, table_emprunts, emprunt_id):
    table = dynamodb.Table(table_emprunts)
    try:
        response = table.get_item(Key={'emprunt_id': emprunt_id})
        return response.get('Item', None)
    except Exception as e:
        print(f"Erreur lors de la récupération de l'emprunt : {e}")

# Lister les emprunts d'un utilisateur
def emprunts_par_utilisateur(dynamodb, table_emprunt, utilisateur):
    table = dynamodb.Table(table_emprunt)
    try:
        response = table.scan(
            FilterExpression="utilisateur = :u",
            ExpressionAttributeValues={':u': utilisateur}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Erreur lors de la récupération des emprunts : {e}")

# Lister les emprunts en retard (plus de 30 jours)
def emprunts_en_retard(dynamodb, table_emprunts):
    table = dynamodb.Table(table_emprunts)
    try:
        date_limite = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        response = table.scan(
            FilterExpression="date_retour = :null AND date_emprunt < :limite",
            ExpressionAttributeValues={':null': None, ':limite': date_limite}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Erreur lors de la récupération des emprunts en retard : {e}")

# Retourner un livre
def retourner_livre(dynamodb, table_livres, table_emprunts, emprunt_id):
    table_livres = dynamodb.Table(table_livres)
    table_emprunts = dynamodb.Table(table_emprunts)

    # Vérifier l'existence de l'emprunt
    emprunt = table_emprunts.get_item(Key={'emprunt_id': emprunt_id}).get('Item')
    if not emprunt or emprunt['date_retour']:
        print("Emprunt introuvable ou déjà retourné.")
        return

    date_retour = datetime.now().strftime("%Y-%m-%d")

    try:
        # Mettre à jour l'emprunt avec la date de retour
        table_emprunts.update_item(
            Key={'emprunt_id': emprunt_id},
            UpdateExpression="SET date_retour = :r",
            ExpressionAttributeValues={':r': date_retour}
        )
        # Rendre le livre disponible à nouveau
        table_livres.update_item(
            Key={'ISBN': emprunt['ISBN']},
            UpdateExpression="SET disponible = :d",
            ExpressionAttributeValues={':d': True}
        )
        print(f"Livre {emprunt['ISBN']} retourné avec succès !")
    except Exception as e:
        print(f"Erreur lors du retour du livre : {e}")

# Trouver tous les livres d'un auteur donné
def livres_par_auteur(dynamodb, table_livres, auteur):
    try:
        table_livres = dynamodb.Table(table_livres)
        response = table_livres.scan(
            FilterExpression="auteur = :a",
            ExpressionAttributeValues={':a': auteur}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Erreur lors de la récupération des livres par auteur : {e}")

# Lister tous les emprunts qui dépassent une certaine durée (ex: 30 jours)
def emprunts_depasse_duree(dynamodb, table_emprunts, duree_jours):
    try:
        table_emprunts = dynamodb.Table(table_emprunts)
        date_limite = (datetime.now() - timedelta(days=duree_jours)).strftime("%Y-%m-%d")
        response = table_emprunts.scan(
            FilterExpression="date_retour = :null AND date_emprunt < :limite",
            ExpressionAttributeValues={':null': None, ':limite': date_limite}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Erreur lors de la récupération des emprunts en retard : {e}")

# Trouver les livres les plus empruntés
def livres_plus_empruntes(dynamodb, table_emprunts, n_top=5):
    try:
        table_emprunts = dynamodb.Table(table_emprunts)
        response = table_emprunts.scan()
        emprunts = response.get('Items', [])

        # Compter le nombre d'emprunts par ISBN
        compteur = Counter([e['ISBN'] for e in emprunts])
        livres_tries = compteur.most_common(n_top)  # Top N des livres les plus empruntés

        return livres_tries
    except Exception as e:
        print(f"Erreur lors du calcul des livres les plus empruntés : {e}")

######

'''

######
def ajouter_livre(dynamodb, table_livres):
    table_livres = dynamodb.Table(table_livres)
    isbn = input("ISBN : ")
    titre = input("Titre : ")
    auteur = input("Auteur : ")
    annee = int(input("Année de publication : "))

    table_livres.put_item(Item={
        'ISBN': isbn,
        'titre': titre,
        'auteur': auteur,
        'annee_publication': annee,
        'disponible': True
    })
    print("✅ Livre ajouté !")

def recuperer_livre(dynamodb, table_livres):
    table_livres = dynamodb.Table(table_livres)
    isbn = input("ISBN du livre : ")
    livre = table_livres.get_item(Key={'ISBN': isbn}).get('Item')
    print(livre if livre else "❌ Livre non trouvé.")

def lister_livres(dynamodb, table_livres):
    table_livres = dynamodb.Table(table_livres)
    livres = table_livres.scan().get('Items', [])
    for livre in livres:
        print(livre)

def supprimer_livre(dynamodb, table_livres):
    table_livres = dynamodb.Table(table_livres)
    isbn = input("ISBN du livre à supprimer : ")
    table_livres.delete_item(Key={'ISBN': isbn})
    print("✅ Livre supprimé !")

# ========================== #
#  GESTION DES EMPRUNTS      #
# ========================== #
def emprunter_livre(dynamodb, table_livres, table_emprunts):
    table_livres = dynamodb.Table(table_livres)
    table_emprunts = dynamodb.Table(table_emprunts)
    isbn = input("ISBN du livre : ")
    utilisateur = input("Nom de l'utilisateur : ")

    livre = table_livres.get_item(Key={'ISBN': isbn}).get('Item')
    if not livre or not livre['disponible']:
        print("❌ Livre indisponible.")
        return

    emprunt_id = str(uuid.uuid4())
    date_emprunt = datetime.now().strftime("%Y-%m-%d")

    table_emprunts.put_item(Item={
        'emprunt_id': emprunt_id,
        'ISBN': isbn,
        'utilisateur': utilisateur,
        'date_emprunt': date_emprunt,
        'date_retour': None
    })
    table_livres.update_item(
        Key={'ISBN': isbn},
        UpdateExpression="SET disponible = :d",
        ExpressionAttributeValues={':d': False}
    )
    print("✅ Livre emprunté !")

def retourner_livre(dynamodb, table_emprunts):
    table_emprunts = dynamodb.Table(table_emprunts)
    emprunt_id = input("ID de l'emprunt : ")
    response = table_emprunts.get_item(Key={'emprunt_id': emprunt_id})
    emprunt = response.get('Item')  # Assurez-vous d'extraire correctement l'Item

    if not emprunt:
        print("❌ Emprunt introuvable.")
        return

    if emprunt.get('date_retour') is not None:
        print("❌ Livre déjà retourné.")
        return

    # Mise à jour de l'emprunt avec la date de retour
    table_emprunts.update_item(
        Key={'emprunt_id': emprunt_id},
        UpdateExpression="SET date_retour = :r",
        ExpressionAttributeValues={':r': datetime.now().strftime("%Y-%m-%d")}
    )

    # Mise à jour du statut du livre
    table_livres.update_item(
        Key={'ISBN': emprunt['ISBN']},
        UpdateExpression="SET disponible = :d",
        ExpressionAttributeValues={':d': True}
    )
    print("✅ Livre retourné avec succès !")

def emprunts_par_utilisateur(dynamodb, table_emprunts):
    table_emprunts = dynamodb.Table(table_emprunts)
    utilisateur = input("Nom de l'utilisateur : ")
    emprunts = table_emprunts.scan(
        FilterExpression="utilisateur = :u",
        ExpressionAttributeValues={':u': utilisateur}
    ).get('Items', [])
    
    for emprunt in emprunts:
        print(emprunt)

# ========================== #
#  REQUÊTES AVANCÉES         #
# ========================== #
def livres_par_auteur(dynamodb, table_livres):
    table_livres = dynamodb.Table(table_livres)
    auteur = input("Nom de l'auteur : ")
    livres = table_livres.scan(
        FilterExpression="auteur = :a",
        ExpressionAttributeValues={':a': auteur}
    ).get('Items', [])
    
    for livre in livres:
        print(livre)

def emprunts_depasse_duree(dynamodb, table_emprunts):
    table_emprunts = dynamodb.Table(table_emprunts)
    duree = int(input("Durée en jours : "))
    date_limite = (datetime.now() - timedelta(days=duree)).strftime("%Y-%m-%d")
    emprunts = table_emprunts.scan(
        FilterExpression="date_retour = :null AND date_emprunt < :limite",
        ExpressionAttributeValues={':null': None, ':limite': date_limite}
    ).get('Items', [])

    for emprunt in emprunts:
        print(emprunt)

def livres_plus_empruntes(dynamodb, table_emprunts):
    table_emprunts = dynamodb.Table(table_emprunts)
    response = table_emprunts.scan().get('Items', [])
    compteur = Counter([e['ISBN'] for e in response])
    top_livres = compteur.most_common(5)

    for livre, count in top_livres:
        print(f"{livre} - {count} emprunts")

# ========================== #
#  MENU CLI                  #
# ========================== #
def menu(dynamodb, table_livres, table_emprunts):
    while True:
        print("\n📚 GESTION DE BIBLIOTHÈQUE 📚")
        print("1️⃣  Ajouter un livre")
        print("2️⃣  Voir un livre")
        print("3️⃣  Lister tous les livres")
        print("4️⃣  Supprimer un livre")
        print("5️⃣  Emprunter un livre")
        print("6️⃣  Retourner un livre")
        print("7️⃣  Voir les emprunts d'un utilisateur")
        print("8️⃣  Livres d'un auteur")
        print("9️⃣  Emprunts en retard")
        print("🔟 Livres les plus empruntés")
        print("0️⃣  Quitter")

        choix = input("👉 Choisissez une option : ")
        if choix == "1":
            ajouter_livre(dynamodb, table_livres)
        elif choix == "2":
            recuperer_livre(dynamodb, table_livres)
        elif choix == "3":
            lister_livres(dynamodb, table_livres)
        elif choix == "4":
            supprimer_livre(dynamodb, table_livres)
        elif choix == "5":
            emprunter_livre(dynamodb, table_livres, table_emprunts)
        elif choix == "6":
            retourner_livre(dynamodb, table_emprunts)
        elif choix == "7":
            emprunts_par_utilisateur(dynamodb, table_emprunts)
        elif choix == "8":
            livres_par_auteur(dynamodb, table_livres)
        elif choix == "9":
            emprunts_depasse_duree(dynamodb, table_emprunts)
        elif choix == "10":
            livres_plus_empruntes(dynamodb, table_emprunts)
        elif choix == "0":
            print("👋 Au revoir !")
            break
        else:
            print("❌ Choix invalide, réessayez.")

if __name__ == "__main__":
    dynamodb = create_dynamodb_resource()
    table_livres = 'livres'
    table_emprunts = 'emprunts'
    '''#table1=dynamodb.Table(table_livre)
    #table1.delete()
    #table1.wait_until_not_exists()
    #table2=dynamodb.Table(table_emprunt)
    #table2.delete()
    #table2.wait_until_not_exists()
    
    #Vérification de l'existance des tables
    if (check_table_exists(dynamodb, table_livres)==False): 
        create_livres_table(dynamodb)
    else: 
        print("la table "+table_livres+" existe déjà\n\n")
    
    
    if (check_table_exists(dynamodb, table_emprunts)==False): 
        create_emprunts_table(dynamodb)
    else: 
        print("la table "+table_emprunts+" existe déjà\n\n")
    
    ajouter_livre(dynamodb, table_livres, "978-1234567890", "Python DynamoDB", "Alice Dupont", 2023)
    print(recuperer_livre(dynamodb, table_livres, "978-1234567890"))
    print(lister_livres(dynamodb, table_livres))
    print("")
    #mettre_a_jour_disponibilite(dynamodb, table_livres, "978-1234567890", False)
    #print(recuperer_livre(dynamodb, table_livres, "978-1234567890"))
    #supprimer_livre(dynamodb, table_livres, "978-1234567890")
    
    # Test emprunt
    emprunter_livre(dynamodb, table_livres, table_emprunts, "978-1234567890", "Jean Dupont")
    print(recuperer_emprunts(dynamodb, table_emprunts, "emprunt_id_test"))
    print(emprunts_par_utilisateur(dynamodb, table_emprunts, "Jean Dupont"))
    print(emprunts_en_retard(dynamodb, table_emprunts))
    retourner_livre(dynamodb, table_livres, table_emprunts, "emprunt_id_test")
    print("")

    #Autres tests
    print(livres_par_auteur(dynamodb, table_livres, "Alice Dupont"))
    print(emprunts_depasse_duree(dynamodb, table_emprunts, 30))
    print(livres_plus_empruntes(dynamodb, table_emprunts, 3))'''
    

    menu(dynamodb, table_livres, table_emprunts)






