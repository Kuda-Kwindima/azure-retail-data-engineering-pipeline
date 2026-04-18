import pandas as pd
import csv

df = pd.read_csv("data/raw/aisles.csv")

df["aisle_id"] = pd.to_numeric(df["aisle_id"], errors="coerce").astype("Int64")
df["aisle"] = df["aisle"].astype(str).str.strip()

df.to_csv(
    "data/staging/clean/aisles_clean.csv",
    index=False,
    quoting=csv.QUOTE_ALL
)