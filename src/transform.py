import pandas as pd
import os
from dotenv import load_dotenv
from src.storage.adls_client import ADLSClient

load_dotenv()


def get_client() -> ADLSClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_CONTAINER")

    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set")

    if not container:
        raise ValueError("AZURE_CONTAINER is not set")

    return ADLSClient(conn_str, container)


def build_dim_products(client: ADLSClient) -> None:
    products = client.read_csv("processed/products/products_clean.csv")
    aisles = client.read_csv("processed/aisles/aisles_clean.csv")
    departments = client.read_csv("processed/departments/departments_clean.csv")

    dim_products = (
        products.merge(aisles, on="aisle_id", how="left")
        .merge(departments, on="department_id", how="left")
    )

    dim_products = dim_products[
        ["product_id", "product_name", "aisle_id", "aisle", "department_id", "department"]
    ]

    client.write_csv(dim_products, "warehouse/dim_products/dim_products.csv")
    print("dim_products built:", dim_products.shape)


def build_dim_aisles(client: ADLSClient) -> None:
    aisles = client.read_csv("processed/aisles/aisles_clean.csv")
    client.write_csv(aisles, "warehouse/dim_aisles/dim_aisles.csv")
    print("dim_aisles built:", aisles.shape)


def build_dim_departments(client: ADLSClient) -> None:
    departments = client.read_csv("processed/departments/departments_clean.csv")
    client.write_csv(departments, "warehouse/dim_departments/dim_departments.csv")
    print("dim_departments built:", departments.shape)


def build_fact_orders_sample(client: ADLSClient) -> None:
    orders = client.read_csv("processed/orders/orders_clean_sample.csv")

    fact_orders = orders[
        [
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ]
    ].copy()

    client.write_csv(fact_orders, "warehouse/fact_orders_sample/fact_orders_sample.csv")
    print("fact_orders_sample built:", fact_orders.shape)

def build_fact_order_items_sample(client: ADLSClient) -> None:
    order_products = client.read_csv("raw/order_products/order_products_sample.csv")
    fact_orders = client.read_csv("warehouse/fact_orders_sample/fact_orders_sample.csv")
    dim_products = client.read_csv("warehouse/dim_products/dim_products.csv")

    fact_order_items = (
        order_products
        .merge(
            fact_orders[
                [
                    "order_id",
                    "user_id",
                    "eval_set",
                    "order_number",
                    "order_dow",
                    "order_hour_of_day",
                    "days_since_prior_order",
                ]
            ],
            on="order_id",
            how="left",
        )
        .merge(
            dim_products[
                [
                    "product_id",
                    "product_name",
                    "aisle_id",
                    "aisle",
                    "department_id",
                    "department",
                ]
            ],
            on="product_id",
            how="left",
        )
    )

    fact_order_items = fact_order_items[
        [
            "order_id",
            "product_id",
            "user_id",
            "product_name",
            "aisle_id",
            "aisle",
            "department_id",
            "department",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
            "add_to_cart_order",
            "reordered",
        ]
    ]

    client.write_csv(
        fact_order_items,
        "warehouse/fact_order_items_sample/fact_order_items_sample.csv",
    )
    print("fact_order_items_sample built:", fact_order_items.shape)

def build_mart_product_reorders(client: ADLSClient) -> None:
    fact_order_items = client.read_csv(
        "warehouse/fact_order_items_sample/fact_order_items_sample.csv"
    )
    dim_products = client.read_csv("warehouse/dim_products/dim_products.csv")

    # --- aggregate ---
    mart_product_reorders = (
        fact_order_items.groupby("product_id")
        .agg(
            total_orders=("product_id", "count"),
            reorder_count=("reordered", "sum"),
        )
        .reset_index()
    )

    # --- ensure numeric FIRST ---
    mart_product_reorders["total_orders"] = pd.to_numeric(
        mart_product_reorders["total_orders"], errors="coerce"
    )

    mart_product_reorders["reorder_count"] = pd.to_numeric(
        mart_product_reorders["reorder_count"], errors="coerce"
    )

    # --- calculate reorder_rate ---
    mart_product_reorders["reorder_rate"] = (
        mart_product_reorders["reorder_count"] /
        mart_product_reorders["total_orders"]
    )

    mart_product_reorders["reorder_rate"] = pd.to_numeric(
        mart_product_reorders["reorder_rate"], errors="coerce"
    )

    # --- calculate score (ONLY ONCE) ---
    mart_product_reorders["score"] = (
        mart_product_reorders["total_orders"] *
        mart_product_reorders["reorder_rate"]
    ).fillna(0)

    # --- merge product names ---
    mart_product_reorders = mart_product_reorders.merge(
        dim_products[["product_id", "product_name"]],
        on="product_id",
        how="left",
    )

    # --- final columns ---
    mart_product_reorders = mart_product_reorders[
        [
            "product_id",
            "product_name",
            "total_orders",
            "reorder_count",
            "reorder_rate",
            "score",
        ]
    ].sort_values("score", ascending=False)

    # --- write ---
    client.write_csv(
        mart_product_reorders,
        "analytics/mart_product_reorders/mart_product_reorders.csv",
    )

    print("mart_product_reorders built:", mart_product_reorders.shape)  

def build_mart_department_reorders(client: ADLSClient) -> None:
    fact_order_items = client.read_csv(
        "warehouse/fact_order_items_sample/fact_order_items_sample.csv"
    )

    mart_department_reorders = (
        fact_order_items.groupby(["department_id", "department"])
        .agg(
            total_orders=("product_id", "count"),
            reorder_count=("reordered", "sum"),
        )
        .reset_index()
    )

    mart_department_reorders["reorder_rate"] = (
        mart_department_reorders["reorder_count"] /
        mart_department_reorders["total_orders"]
    )

    mart_department_reorders["score"] = (
        mart_department_reorders["total_orders"] *
        mart_department_reorders["reorder_rate"]
    )

    mart_department_reorders = mart_department_reorders[
        [
            "department_id",
            "department",
            "total_orders",
            "reorder_count",
            "reorder_rate",
            "score",
        ]
    ].sort_values("score", ascending=False)

    client.write_csv(
        mart_department_reorders,
        "analytics/mart_department_reorders/mart_department_reorders.csv",
    )

    print("mart_department_reorders built:", mart_department_reorders.shape)

def build_mart_customer_orders(client: ADLSClient) -> None:
    orders = client.read_csv("warehouse/fact_orders_sample/fact_orders_sample.csv")
    order_items = client.read_csv("warehouse/fact_order_items_sample/fact_order_items_sample.csv")

    # --- total orders per user ---
    user_orders = (
        orders.groupby("user_id")
        .agg(total_orders=("order_id", "nunique"),
             avg_days_between_orders=("days_since_prior_order", "mean"))
        .reset_index()
    )

    # --- total products per user ---
    user_products = (
        order_items.groupby("user_id")
        .agg(total_products_ordered=("product_id", "count"),
             reorder_rate=("reordered", "mean"))
        .reset_index()
    )

    # --- merge ---
    mart_customers = user_orders.merge(user_products, on="user_id", how="left")

    # --- derived metric ---
    mart_customers["avg_products_per_order"] = (
        mart_customers["total_products_ordered"] / mart_customers["total_orders"]
    )

    # --- save ---
    client.write_csv(
        mart_customers,
        "analytics/mart_customer_orders/mart_customer_orders.csv"
    )

    print("mart_customer_orders built:", mart_customers.shape)

def main() -> None:
    client = get_client()

    build_dim_products(client)
    build_dim_aisles(client)
    build_dim_departments(client)
    build_fact_orders_sample(client)
    build_fact_order_items_sample(client)
    build_mart_product_reorders(client)
    build_mart_department_reorders(client)
    build_mart_customer_orders(client)

    print("Warehouse transform completed successfully.")

if __name__ == "__main__":
    main()