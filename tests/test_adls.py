import os

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
def test_read_orders_from_adls():
    client = ADLSClient(conn_str, container)

    df = client.read_csv("raw/orders/orders.csv")

    assert df is not None
    assert not df.empty
    assert "order_id" in df.columns