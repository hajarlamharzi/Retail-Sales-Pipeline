from pymongo import MongoClient

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Sélection de la base de données
db = client["retail_sales_db"]

# Test de connexion
try:
    client.admin.command("ping")
    print("Connexion à MongoDB réussie !")

    print("Base de données :", db.name)

except Exception as e:
    print("Erreur de connexion à MongoDB :", e)

finally:
    client.close()