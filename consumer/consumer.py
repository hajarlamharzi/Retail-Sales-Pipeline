import json
import pandas as pd

from kafka import KafkaConsumer
from pymongo import MongoClient


consumer = KafkaConsumer(
    "retail_sales",
    bootstrap_servers="kafka:9092",
    group_id="retail_sales_group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

transactions = []
print("Consumer démarré. Lecture des transactions...")

for message in consumer:
    transaction = message.value
    transactions.append(transaction)

    if len(transactions) % 1000 == 0:
        print(f"{len(transactions)} transactions reçues")

    if len(transactions) == 18045:
        break
df = pd.DataFrame(transactions)

print("\nNombre de transactions reçues :", len(df))
print("Nombre de colonnes :", len(df.columns))

print("\nAvant nettoyage :", len(df), "lignes")

df = df.drop_duplicates()

print("Après suppression des doublons :", len(df), "lignes")
df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")

df["Customer_Name"] = df["Customer_Name"].fillna("Unknown")
df["Customer_Gender"] = df["Customer_Gender"].fillna("Unknown")
df["Product_Name"] = df["Product_Name"].fillna("Unknown")
df["Payment_Method"] = df["Payment_Method"].fillna("Unknown")
df["Promotion_Code"] = df["Promotion_Code"].fillna("No_Promotion")

df["Customer_Age"] = df["Customer_Age"].fillna(39)
df["Delivery_Days"] = df["Delivery_Days"].fillna(1)
df["Customer_Rating"] = df["Customer_Rating"].fillna(4)
df["Inventory_Level"] = df["Inventory_Level"].fillna(70)

print("\nValeurs manquantes après nettoyage :")
print(df.isnull().sum())

df["Product_Category"] = df["Product_Category"].str.strip().str.title()

sales_by_category = (
    df.groupby("Product_Category")["Sales_Amount"]
      .sum()
      .reset_index()
)
print("\nTotal Sales par catégorie :")
print(sales_by_category)

df["Region"] = df["Region"].str.strip()

df["Region"] = df["Region"].replace({
    "British columbia": "British Columbia"
})

# Traitement des valeurs manquantes de Return_Reason
df["Return_Reason"] = df["Return_Reason"].fillna("Not_Returned")

# Colonnes utiles pour notre projet
useful_columns = [
    "Transaction_ID",
    "Order_ID",
    "Customer_ID",
    "Order_Date",
    "Sales_Channel",
    "Country",
    "Region",
    "City",
    "Product_ID",
    "Product_Name",
    "Product_Category",
    "Product_Subcategory",
    "Quantity",
    "Unit_Price",
    "Discount_Percentage",
    "Sales_Amount",
    "Cost_Amount",
    "Profit",
    "Payment_Method",
    "Order_Status",
    "Delivery_Days",
    "Return_Flag",
    "Return_Reason",
    "Customer_Rating",
    "Order_Year"
]

df = df[useful_columns]

print("\nAprès filtrage des colonnes :")
print("Nombre de lignes :", len(df))
print("Nombre de colonnes :", len(df.columns))

print("\nValeurs manquantes après nettoyage :")
print(df.isnull().sum().sum())


profit_by_region = (
    df.groupby("Region")["Profit"]
      .sum()
      .reset_index()
)

print("\nTotal Profit par région :")
print(profit_by_region)

rating_by_category = (
    df.groupby("Product_Category")["Customer_Rating"]
      .mean()
      .reset_index()
)

print("\nAverage Customer Rating par catégorie :")
print(rating_by_category)

quantity_by_year = (
    df.groupby("Order_Year")["Quantity"]
      .sum()
      .reset_index()
)

print("\nTotal Quantity vendue par année :")
print(quantity_by_year)

# =========================
# Connexion à MongoDB
# =========================

client = MongoClient("mongodb://mongodb:27017/")
db = client["retail_sales_db"]

sales_collection = db["sales_transactions"]
analytics_collection = db["sales_analytics"]

try:
    # Test de connexion
    client.admin.command("ping")
    print("\nConnexion à MongoDB réussie !")

    # =========================
    # Insertion des transactions
    # =========================

    transactions = df.to_dict(orient="records")

    sales_collection.delete_many({})

    if transactions:
        sales_collection.insert_many(transactions)

    print("Transactions insérées :", len(transactions))

   
   


    analytics_collection.delete_many({})

    analytics_documents = [
        {
            "type": "sales_by_category",
            "data": sales_by_category.to_dict(orient="records")
        },
        {
            "type": "profit_by_region",
            "data": profit_by_region.to_dict(orient="records")
        },
        {
            "type": "rating_by_category",
            "data": rating_by_category.to_dict(orient="records")
        },
        {
            "type": "quantity_by_year",
            "data": quantity_by_year.to_dict(orient="records")
        }
    ]

    analytics_collection.insert_many(analytics_documents)

    print("Analytics insérées :", len(analytics_documents))

except Exception as e:
    print("\nErreur MongoDB :", e)

finally:
    client.close()