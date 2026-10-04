import pandas as pd

file_path = "data/Sales_transactions_2022_2025.csv"

df = pd.read_csv(file_path)

print("Nombre de lignes :", df.shape[0])
print("Nombre de colonnes :", df.shape[1])

print("\n--- Return Flag ---")
print(df["Return_Flag"].value_counts(dropna=False))

print("\n--- Return Flag + Return Reason ---")
print(
    pd.crosstab(
        df["Return_Flag"],
        df["Return_Reason"].isna()
    )
)

print("\n--- Doublons ---")
print("Nombre de doublons :", df.duplicated().sum())

print("\n--- Exemples de doublons ---")
print(df[df.duplicated(keep=False)].head(10))