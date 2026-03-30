from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO, StringIO


class ADLSClient:
    def __init__(self, connection_string: str, container_name: str):
        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name

    def read_csv(self, blob_path: str, nrows=None) -> pd.DataFrame:
        blob_client = self.blob_service.get_blob_client(
            container=self.container_name,
            blob=blob_path,
        )
        data = blob_client.download_blob(max_concurrency=4).readall()
        return pd.read_csv(BytesIO(data), nrows=nrows)

    def write_csv(self, df: pd.DataFrame, blob_path: str) -> None:
        blob_client = self.blob_service.get_blob_client(
            container=self.container_name,
            blob=blob_path,
        )

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        data = buffer.getvalue().encode("utf-8")

        blob_client.upload_blob(
            data,
            overwrite=True,
            timeout=300,
            max_concurrency=2,
        )