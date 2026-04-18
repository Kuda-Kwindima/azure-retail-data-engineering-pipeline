import pandas as pd
import csv

df = pd.read_csv("data/raw/products.csv")

df["product_name"] = (
    df["product_name"]
    .astype(str)
    .str.replace('\\', '', regex=False)   # REMOVE backslashes
    .str.replace('"', '', regex=False)    # REMOVE quotes
    .str.replace(',', ' ', regex=False)   # REMOVE commas inside text
)

df.to_csv(
    "data/staging/products_clean.csv",
    index=False,
    quoting=csv.QUOTE_ALL
)