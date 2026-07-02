import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from src.storage.adls_client import ADLSClient


load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container = os.getenv("AZURE_CONTAINER")


@pytest.mark.skipif(
    not conn_str or not container,
    reason="Azure credentials are not configured.",
)
def test_write_sample_orders_to_adls():
    client = ADLSClient(conn_str, container)

    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "user_id": [10, 20, 30],
            "eval_set": ["prior", "train", "prior"],
            "order_number": [1, 2, 3],
            "order_dow": [0, 1, 2],
            "order_hour_of_day": [8, 12, 18],
            "days_since_prior_order": [0.0, 7.0, 14.0],
        }
    )

    client.write_csv(df, "processed/orders/orders_sample.csv")

    assert len(df) == 3
    assert list(df.columns) == [
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    ]