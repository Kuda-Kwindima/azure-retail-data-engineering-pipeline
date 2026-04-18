import pandas as pd
import csv

df = pd.read_csv("data/raw/departments.csv")

df["department_id"] = pd.to_numeric(df["department_id"], errors="coerce").astype("Int64")
df["department"] = df["department"].astype(str).str.strip()

df.to_csv(
    "data/staging/clean/departments_clean.csv",
    index=False,
    quoting=csv.QUOTE_ALL
)