import pandas as pd
import csv

df = pd.read_csv("data/raw/orders.csv")

int_cols = ["order_id", "user_id", "order_number", "order_dow", "order_hour_of_day"]
for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

df["eval_set"] = df["eval_set"].astype(str).str.strip()

df["days_since_prior_order"] = pd.to_numeric(
    df["days_since_prior_order"], errors="coerce"
)

df.to_csv(
    "data/staging/clean/orders_clean.csv",
    index=False,
    quoting=csv.QUOTE_MINIMAL
)