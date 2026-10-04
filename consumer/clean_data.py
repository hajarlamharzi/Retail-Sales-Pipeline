import pandas as pd

# 1. Lire le fichier CSV
file_path = "data/Sales_transactions_2022_2025.csv"
df = pd.read_csv(file_path)

print("Avant nettoyage :", len(df), "lignes")


# 2. Supprimer les doublons
df = df.drop_duplicates()

print("Après suppression des doublons :", len(df), "lignes")


# 3. Convertir Order_Date en datetime
df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")

print("\nType de Order_Date :", df["Order_Date"].dtype)


# 4. Traiter les valeurs manquantes

# Colonnes textuelles
df["Customer_Name"] = df["Customer_Name"].fillna("Unknown")
df["Customer_Gender"] = df["Customer_Gender"].fillna("Unknown")
df["Product_Name"] = df["Product_Name"].fillna("Unknown")
df["Payment_Method"] = df["Payment_Method"].fillna("Unknown")

# Promotion absente = aucune promotion
df["Promotion_Code"] = df["Promotion_Code"].fillna("No_Promotion")

# Colonnes numériques : remplacement par la médiane
df["Customer_Age"] = df["Customer_Age"].fillna(39)
df["Delivery_Days"] = df["Delivery_Days"].fillna(1)
df["Customer_Rating"] = df["Customer_Rating"].fillna(4)
df["Inventory_Level"] = df["Inventory_Level"].fillna(70)

# Return_Reason :
# On garde les valeurs manquantes car elles correspondent
# aux transactions sans retour.


# 5. Vérifier les valeurs manquantes après nettoyage
print("\nValeurs manquantes après nettoyage :")
print(df.isnull().sum())


# 6. Vérifier les doublons
print("\nDoublons après nettoyage :", df.duplicated().sum())


# 7. Nombre final de lignes
print("\nNombre final de lignes :", len(df))