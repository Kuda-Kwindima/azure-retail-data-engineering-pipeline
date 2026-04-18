import pandas as pd
import csv

df = pd.read_csv("data/raw/order_products__train.csv")

for col in ["order_id", "product_id", "add_to_cart_order", "reordered"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

df.to_csv(
    "data/staging/clean/order_products_train_clean.csv",
    index=False,
    quoting=csv.QUOTE_ALL
)