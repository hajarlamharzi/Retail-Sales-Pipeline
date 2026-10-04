# Retail Sales Pipeline

## Sommaire

1. [Présentation du projet](#1-présentation-du-projet)
2. [Dataset](#2-dataset)
3. [Architecture du pipeline](#3-architecture-du-pipeline)
4. [Structure du projet](#4-structure-du-projet)
5. [Extraction et lecture des données](#5-extraction-et-lecture-des-données)
6. [Kafka Producer](#6-kafka-producer)
7. [Kafka Consumer](#7-kafka-consumer)
8. [Nettoyage et préparation des données](#8-nettoyage-et-préparation-des-données)
9. [Agrégation et calculs](#9-agrégation-et-calculs)
10. [MongoDB](#10-mongodb)
11. [Dockerisation](#11-dockerisation)
12. [Exécution du projet](#12-exécution-du-projet)
13. [Vérification des résultats](#13-vérification-des-résultats)
14. [Résultats obtenus](#14-résultats-obtenus)

---

## 1. Présentation du projet

Ce projet consiste à mettre en place un pipeline de traitement de données de ventes à partir d'un fichier CSV contenant des transactions de vente.

Le pipeline permet de :

* lire les données de ventes à partir d'un fichier CSV ;
* transmettre les transactions à Kafka à l'aide d'un Producer Python ;
* récupérer les messages avec un Consumer Python ;
* nettoyer et préparer les données ;
* supprimer les doublons ;
* traiter les valeurs manquantes ;
* convertir et normaliser certaines données ;
* filtrer les colonnes utiles ;
* effectuer plusieurs agrégations ;
* enregistrer les données nettoyées et les résultats des agrégations dans MongoDB ;
* exécuter l'ensemble des composants dans des conteneurs Docker.

Le pipeline réalisé est le suivant :

```text
Sales_transactions_2022_2025.csv
              │
              ▼
      Python Kafka Producer
              │
              ▼
            Kafka
       Topic: retail_sales
              │
              ▼
      Python Kafka Consumer
              │
              ▼
    Nettoyage des données
              │
              ▼
       Filtrage des colonnes
              │
              ▼
        Agrégations
              │
              ▼
           MongoDB
```

---

## 2. Dataset

Le projet utilise le fichier `Sales_transactions_2022_2025.csv`, placé dans le dossier `data/`.

### Format

* Format : CSV
* Nombre initial de lignes : **18 045**
* Nombre initial de colonnes : **36**

### Colonnes disponibles dans le dataset initial

```text
Transaction_ID
Order_ID
Customer_ID
Customer_Name
Customer_Age
Customer_Gender
Customer_Segment
Order_Date
Order_Time
Sales_Channel
Store_ID
Store_Name
Country
Region
City
Product_ID
Product_Name
Product_Category
Product_Subcategory
Quantity
Unit_Price
Discount_Percentage
Sales_Amount
Cost_Amount
Profit
Payment_Method
Order_Status
Shipping_Method
Delivery_Days
Return_Flag
Return_Reason
Sales_Representative
Promotion_Code
Customer_Rating
Inventory_Level
Order_Year
```

Le dataset contient donc des informations concernant les transactions, les clients, les produits, les ventes, les régions, les paiements, les retours, les évaluations et les années de commande.

---

## 3. Architecture du pipeline

L'architecture mise en place est composée de plusieurs étapes :

```text
                         ┌──────────────────────────┐
                         │         CSV Dataset      │
                         │ Sales_transactions_...   │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │     Python Producer      │
                         │       pandas + Kafka     │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │          Kafka           │
                         │    Topic: retail_sales   │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │     Python Consumer      │
                         │       Kafka + pandas     │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │ Nettoyage des données    │
                         │ - Doublons               │
                         │ - Valeurs manquantes     │
                         │ - Dates                  │
                         │ - Normalisation          │
                         │ - Filtrage des colonnes  │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │      Agrégations         │
                         │ - Sales / catégorie      │
                         │ - Profit / région        │
                         │ - Rating / catégorie     │
                         │ - Quantity / année       │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │         MongoDB          │
                         │   retail_sales_db        │
                         └──────────────────────────┘
```

Tous les composants sont exécutés dans Docker à l'aide de Docker Compose.

---

## 4. Structure du projet

La structure finale du projet est la suivante :

```text
Retail-Sales-Pipeline/
│
├── data/
│   └── Sales_transactions_2022_2025.csv
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── consumer.py
│   └── clean_data.py
│
└── docker-compose.yml
```

### Dossier `data/`

Contient le fichier CSV utilisé comme source des données.

### Dossier `producer/`

Contient le programme Python responsable de la lecture du CSV et de l'envoi des transactions vers Kafka.

### Dossier `consumer/`

Contient le Consumer Kafka ainsi que les fonctions utilisées pour le nettoyage et le traitement des données.

### `docker-compose.yml`

Permet de définir et de lancer les différents services du projet :

* Kafka
* MongoDB
* Producer
* Consumer

---

## 5. Extraction et lecture des données

La première étape du traitement consiste à lire le fichier CSV avec la bibliothèque **Pandas**.

Le fichier est chargé avec :

```python
df = pd.read_csv(file_path)
```

Le programme vérifie ensuite le nombre de transactions :

```python
print("Nombre de transactions :", len(df))
```

Le dataset contient initialement :

```text
Nombre de transactions : 18045
```

Chaque ligne du DataFrame représente une transaction.

Dans le Producer, chaque ligne est transformée en dictionnaire Python :

```python
message = row.to_dict()
```

Le dictionnaire est ensuite envoyé à Kafka.

---

## 6. Kafka Producer

Le Producer est développé en Python avec la bibliothèque `kafka-python`.

La connexion à Kafka est configurée avec :

```python
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)
```

Le Producer utilise le service Docker `kafka` comme serveur Kafka.

Le topic utilisé dans le projet est `retail_sales`.

Pour chaque ligne du dataset :

```python
for _, row in df.iterrows():
    message = row.to_dict()
    producer.send("retail_sales", value=message)
```

Chaque transaction est donc transformée en message JSON avant d'être envoyée au topic Kafka.

À la fin de l'envoi, `producer.flush()` permet de s'assurer que les messages en attente sont envoyés.

### Résultat

Le Producer a traité **18 045 transactions** et les a envoyées vers Kafka.

---

## 7. Kafka Consumer

Le Consumer récupère les transactions depuis le topic `retail_sales`.

La connexion est configurée avec :

```python
consumer = KafkaConsumer(
    "retail_sales",
    bootstrap_servers="kafka:9092",
    group_id="retail_sales_group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)
```

Le Consumer :

1. se connecte à Kafka ;
2. lit les messages du topic ;
3. convertit les messages JSON en dictionnaires Python ;
4. récupère les transactions ;
5. reconstruit un DataFrame Pandas ;
6. transmet les données au processus de nettoyage.

Le Consumer a récupéré :

```text
Nombre de transactions reçues : 18045
Nombre de colonnes : 36
```

Les données reçues correspondent donc au dataset initial.

---

## 8. Nettoyage et préparation des données

Après la récupération des transactions, les données sont nettoyées avant leur stockage.

Le nettoyage comprend plusieurs étapes.

### 8.1 Suppression des doublons

Le dataset initial contient 18 045 lignes.

La suppression des doublons est effectuée avec :

```python
df = df.drop_duplicates()
```

Après cette opération :

```text
Avant suppression des doublons : 18 045
Après suppression des doublons : 18 000
```

Donc **45 doublons supprimés**.

---

### 8.2 Conversion et traitement des dates

La colonne `Order_Date` est convertie au format date avec :

```python
df["Order_Date"] = pd.to_datetime(
    df["Order_Date"],
    errors="coerce"
)
```

Cette conversion permet de travailler correctement avec les dates lors du traitement et des analyses.

L'utilisation de `errors="coerce"` permet de transformer une éventuelle valeur de date invalide en valeur manquante plutôt que de provoquer une erreur pendant le traitement.

---

### 8.3 Traitement des valeurs manquantes

Une vérification des valeurs manquantes a été effectuée sur les colonnes du dataset.

Plusieurs colonnes contenaient des valeurs manquantes, notamment :

```text
Customer_Name
Customer_Age
Customer_Gender
Product_Name
Payment_Method
Delivery_Days
Return_Reason
Promotion_Code
Customer_Rating
Inventory_Level
```

Les valeurs textuelles ont été remplacées par des valeurs explicites.

Par exemple :

```python
df["Customer_Name"] = df["Customer_Name"].fillna("Unknown")
df["Customer_Gender"] = df["Customer_Gender"].fillna("Unknown")
df["Product_Name"] = df["Product_Name"].fillna("Unknown")
df["Payment_Method"] = df["Payment_Method"].fillna("Unknown")
df["Promotion_Code"] = df["Promotion_Code"].fillna("No_Promotion")
```

Pour certaines colonnes numériques, les valeurs manquantes ont été remplacées par les médianes calculées à partir des données :

```text
Customer_Age      → 39
Delivery_Days     → 1
Customer_Rating   → 4
Inventory_Level   → 70
```

Le traitement de `Return_Reason` est particulier.

La majorité des valeurs manquantes de cette colonne correspondent aux transactions qui n'ont pas été retournées.

La valeur `Not_Returned` a donc été utilisée pour représenter cette situation :

```python
df["Return_Reason"] = df["Return_Reason"].fillna("Not_Returned")
```

Cela permet de conserver les transactions normales sans les supprimer.

Après le traitement, la vérification finale donne :

```text
Valeurs manquantes après nettoyage : 0
```

---

### 8.4 Normalisation des données

Certaines valeurs textuelles ont été nettoyées afin d'avoir une représentation plus homogène.

Pour les catégories de produits :

```python
df["Product_Category"] = (
    df["Product_Category"]
    .str.strip()
    .str.title()
)
```

Cette opération permet notamment de supprimer les espaces inutiles et d'uniformiser l'écriture des catégories.

La colonne `Region` a été traitée séparément.

Les espaces inutiles sont supprimés :

```python
df["Region"] = df["Region"].str.strip()
```

Une correction spécifique a également été appliquée :

```python
df["Region"] = df["Region"].replace({
    "British columbia": "British Columbia"
})
```

La normalisation automatique avec `.str.title()` n'a pas été appliquée à `Region`, afin de conserver correctement certaines valeurs particulières comme `NRW` et `Île-de-France`.

---

### 8.5 Filtrage des colonnes

Après le nettoyage, seules les colonnes utiles pour le traitement et les analyses sont conservées.

La liste finale contient **25 colonnes** :

```python
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
```

Le filtrage est effectué avec :

```python
df = df[useful_columns]
```

Le dataset passe donc de 36 colonnes à 25 colonnes.

Les informations qui ne sont pas nécessaires pour les traitements réalisés dans le projet sont ainsi retirées du DataFrame final.

---

### 8.6 Vérification finale après nettoyage

Après toutes les opérations de préparation :

```text
Nombre de lignes : 18 000
Nombre de colonnes : 25
Doublons : 0
Valeurs manquantes : 0
```

Les données sont alors prêtes pour les agrégations et le stockage dans MongoDB.

---

## 9. Agrégation et calculs

Après le nettoyage, plusieurs calculs sont réalisés avec Pandas.

Les résultats sont ensuite enregistrés séparément dans MongoDB.

### 9.1 Total des ventes par catégorie

Le total des ventes est calculé avec :

```python
sales_by_category = (
    df.groupby("Product_Category")["Sales_Amount"]
      .sum()
      .reset_index()
)
```

Résultat :

| Product Category | Total Sales |
| ---------------- | ----------: |
| Appliances       |   584089.48 |
| Electronics      |  4922081.42 |
| Furniture        |  1986144.85 |
| Office Supplies  |   741774.85 |

---

### 9.2 Total du profit par région

Le profit total est calculé avec :

```python
profit_by_region = (
    df.groupby("Region")["Profit"]
      .sum()
      .reset_index()
)
```

Résultats :

| Region                 | Total Profit |
| ---------------------- | -----------: |
| Bavaria                |    113943.02 |
| British Columbia       |    105606.23 |
| England                |    269790.34 |
| Midwest                |    142699.05 |
| NRW                    |       373.90 |
| New South Wales        |    101048.71 |
| North East             |       977.83 |
| North Rhine-Westphalia |     75226.93 |
| Northeast              |    200433.45 |
| Ontario                |    135187.48 |
| South                  |    154539.86 |
| West                   |    200950.05 |
| Île-de-France          |    144662.55 |

---

### 9.3 Moyenne des évaluations par catégorie

La moyenne des évaluations clients est calculée avec :

```python
rating_by_category = (
    df.groupby("Product_Category")["Customer_Rating"]
      .mean()
      .reset_index()
)
```

Résultat :

| Product Category | Average Rating |
| ---------------- | -------------: |
| Appliances       |       3.956675 |
| Electronics      |       3.991573 |
| Furniture        |       3.996670 |
| Office Supplies  |       3.990716 |

---

### 9.4 Quantité vendue par année

La quantité totale vendue par année est calculée avec :

```python
quantity_by_year = (
    df.groupby("Order_Year")["Quantity"]
      .sum()
      .reset_index()
)
```

Résultat :

| Year | Total Quantity |
| ---- | -------------: |
| 2022 |           8804 |
| 2023 |           9728 |
| 2024 |           9058 |
| 2025 |          10687 |

---

## 10. MongoDB

MongoDB est utilisé pour stocker les données après leur traitement.

La base de données utilisée est `retail_sales_db`.

Deux collections sont utilisées.

### 10.1 Collection `sales_transactions`

Cette collection contient les transactions après nettoyage.

Elle contient **18 000 documents**. Chaque document représente une transaction nettoyée.

Exemple de document :

```text
{
  Transaction_ID: "T02016694",
  Order_ID: "O0116694",
  Customer_ID: "C01482",
  Order_Date: ISODate("2022-10-06T00:00:00.000Z"),
  Sales_Channel: "Online",
  Country: "United States",
  Region: "West",
  City: "LOS ANGELES",
  Product_ID: "P026",
  Product_Name: "Webcam HD",
  Product_Category: "Electronics",
  Product_Subcategory: "Cameras",
  Quantity: 2,
  Unit_Price: 85.29,
  Discount_Percentage: 5,
  Sales_Amount: 162.05,
  Cost_Amount: 115.61,
  Profit: 46.44,
  Payment_Method: "Credit Card",
  Order_Status: "Completed",
  Delivery_Days: 4,
  Return_Flag: "No",
  Return_Reason: "Not_Returned",
  Customer_Rating: 4,
  Order_Year: 2022
}
```

---

### 10.2 Collection `sales_analytics`

Cette collection contient les résultats des agrégations.

Elle contient **4 documents**, correspondant aux quatre analyses réalisées :

```text
sales_by_category
profit_by_region
rating_by_category
quantity_by_year
```

La structure logique est donc :

```text
retail_sales_db
│
├── sales_transactions
│      └── 18 000 documents
│
└── sales_analytics
       └── 4 documents
```

---

## 11. Dockerisation

L'ensemble du projet a été dockerisé afin d'exécuter les différents composants dans des conteneurs.

Le fichier principal utilisé est `docker-compose.yml`.

Il définit quatre services :

```text
kafka
mongodb
producer
consumer
```

### 11.1 Service Kafka

Kafka utilise l'image `apache/kafka:latest`.

Le port utilisé est `9092`.

Le topic utilisé par le projet est `retail_sales`.

---

### 11.2 Service MongoDB

MongoDB utilise l'image `mongo:7.0`.

Le port exposé sur la machine hôte est `27022`.

Le stockage MongoDB utilise un volume Docker :

```yaml
volumes:
  - mongodb_data:/data/db
```

Cela permet de conserver les données MongoDB dans un volume Docker.

---

### 11.3 Service Producer

Le Producer possède son propre `Dockerfile`.

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
```

Le fichier `requirements.txt` contient :

```text
pandas
kafka-python
```

Le dossier `data` de la machine est monté dans le conteneur :

```yaml
volumes:
  - ./data:/app/data:ro
```

Le Producer peut ainsi accéder au fichier CSV.

---

### 11.4 Service Consumer

Le Consumer possède également son propre `Dockerfile`.

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY clean_data.py .

CMD ["python", "consumer.py"]
```

Les dépendances utilisées sont :

```text
pandas
kafka-python
pymongo
```

Le Consumer communique avec Kafka et MongoDB en utilisant les noms des services Docker :

```text
kafka:9092
mongodb:27017
```

---

## 12. Exécution du projet

### 12.1 Construction des images

Depuis le dossier du projet :

```powershell
docker compose build
```

Cette commande construit les images nécessaires pour le Producer et le Consumer.

---

### 12.2 Démarrage des services

Les services sont démarrés avec :

```powershell
docker compose up -d
```

Les services Kafka et MongoDB restent actifs en arrière-plan.

Le Producer et le Consumer exécutent leurs traitements puis terminent leur programme.

---

### 12.3 Vérification des containers

La commande suivante permet de vérifier l'état des services :

```powershell
docker compose ps
```

Après l'exécution du pipeline, Kafka et MongoDB restent actifs :

```text
retail-kafka
retail-mongodb
```

Le Producer et le Consumer peuvent apparaître comme terminés après avoir fini leur traitement, ce qui est normal pour ces deux services.

---

### 12.4 Exécution du Producer

Le Producer lit le dataset et envoie les transactions vers Kafka.

Le traitement a produit :

```text
Nombre de transactions : 18045
Toutes les transactions ont été envoyées à Kafka.
```

---

### 12.5 Traitement du Consumer

Le Consumer récupère les transactions, effectue le nettoyage, les agrégations et l'enregistrement dans MongoDB.

Le traitement final a produit :

```text
Nombre de transactions reçues : 18045

Avant nettoyage : 18045 lignes
Après suppression des doublons : 18000 lignes

Nombre de lignes après filtrage : 18000
Nombre de colonnes après filtrage : 25

Valeurs manquantes après nettoyage : 0

Transactions insérées : 18000
Analytics insérées : 4
```

---

## 13. Vérification des résultats

Une vérification directe de MongoDB a été effectuée depuis Docker.

### Vérification des transactions

La commande utilisée est :

```powershell
docker exec retail-mongodb mongosh --quiet --eval "db.getSiblingDB('retail_sales_db').sales_transactions.countDocuments()"
```

Résultat :

```text
18000
```

Cela confirme que les **18 000 transactions nettoyées** ont bien été enregistrées dans MongoDB.

---

### Vérification des analytics

La commande utilisée est :

```powershell
docker exec retail-mongodb mongosh --quiet --eval "db.getSiblingDB('retail_sales_db').sales_analytics.countDocuments()"
```

Résultat :

```text
4
```

Cela confirme que les quatre résultats d'agrégation ont également été enregistrés.

---

## 14. Résultats obtenus

Le pipeline complet a été exécuté avec succès.

### Données initiales

```text
18 045 lignes
36 colonnes
```

### Après nettoyage

```text
18 000 lignes
25 colonnes
0 doublon
0 valeur manquante
```

### Transformations réalisées

```text
✓ Lecture du fichier CSV
✓ Conversion des lignes en messages
✓ Envoi vers Kafka
✓ Récupération des messages avec Kafka Consumer
✓ Suppression des doublons
✓ Traitement des valeurs manquantes
✓ Conversion des dates
✓ Normalisation de certaines valeurs
✓ Filtrage des colonnes utiles
✓ Calcul des agrégations
✓ Insertion des transactions dans MongoDB
✓ Insertion des résultats analytiques dans MongoDB
✓ Exécution des composants avec Docker
✓ Vérification finale des données dans MongoDB
```

### Données stockées dans MongoDB

```text
sales_transactions → 18 000 documents
sales_analytics    → 4 documents
```

Le pipeline final fonctionne donc de bout en bout :

```text
CSV
 ↓
Python Producer
 ↓
Kafka
 ↓
Python Consumer
 ↓
Nettoyage
 ↓
Filtrage
 ↓
Agrégation
 ↓
MongoDB
```

L'ensemble des composants nécessaires au traitement est exécuté dans l'environnement Docker défini par `docker-compose.yml`.
