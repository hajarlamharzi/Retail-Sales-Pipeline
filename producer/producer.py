import pandas as pd
import json

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)
file_path = "data/Sales_transactions_2022_2025.csv"

df = pd.read_csv(file_path)

print("Nombre de transactions :", len(df))
for _, row in df.iterrows():
    message = row.to_dict()
    producer.send("retail_sales", value=message)

producer.flush()

print("Toutes les transactions ont été envoyées à Kafka.")
