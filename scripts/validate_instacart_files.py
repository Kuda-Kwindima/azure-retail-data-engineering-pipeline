import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/raw")

FILES = {
    "products.csv": {
        "expected_columns": ["product_id", "product_name", "aisle_id", "department_id"],
        "key_columns": ["product_id"],
    },
    "aisles.csv": {
        "expected_columns": ["aisle_id", "aisle"],
        "key_columns": ["aisle_id"],
    },
    "departments.csv": {
        "expected_columns": ["department_id", "department"],
        "key_columns": ["department_id"],
    },
    "orders.csv": {
        "expected_columns": [
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ],
        "key_columns": ["order_id"],
    },
    "order_products_sample.csv": {
        "expected_columns": ["order_id", "product_id", "add_to_cart_order", "reordered"],
        "key_columns": [],
    },
}


def validate_file(file_name: str, expected_columns: list[str], key_columns: list[str]) -> None:
    file_path = BASE_DIR / file_name
    print(f"\n{'=' * 80}")
    print(f"VALIDATING: {file_name}")
    print(f"PATH: {file_path}")

    if not file_path.exists():
        print("❌ File not found")
        return

    try:
        df = pd.read_csv(file_path)
        print("✅ Read successful")
    except Exception as e:
        print(f"❌ Read failed: {e}")
        return

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Column names: {list(df.columns)}")

    missing_expected = [col for col in expected_columns if col not in df.columns]
    extra_columns = [col for col in df.columns if col not in expected_columns]

    if missing_expected:
        print(f"❌ Missing expected columns: {missing_expected}")
    else:
        print("✅ Expected columns present")

    if extra_columns:
        print(f"⚠️ Extra columns found: {extra_columns}")
    else:
        print("✅ No unexpected columns")

    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0]

    if not null_counts.empty:
        print("⚠️ Null values found:")
        print(null_counts.to_string())
    else:
        print("✅ No null values")

    dup_count = df.duplicated().sum()
    print(f"Duplicate rows: {dup_count:,}")

    if key_columns:
        key_dups = df.duplicated(subset=key_columns).sum()
        print(f"Duplicate key rows on {key_columns}: {key_dups:,}")

    print("\nSample rows:")
    print(df.head(3).to_string(index=False))


def main() -> None:
    for file_name, config in FILES.items():
        validate_file(
            file_name=file_name,
            expected_columns=config["expected_columns"],
            key_columns=config["key_columns"],
        )


if __name__ == "__main__":
    main()