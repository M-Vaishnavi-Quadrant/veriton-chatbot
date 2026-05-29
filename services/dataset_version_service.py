from azure.storage.blob import BlobServiceClient
from io import BytesIO
import pandas as pd

from config import (
    V_CONNECTION_STRING,
    V_DATASET_CONTAINER
)


class DatasetVersionService:

    def __init__(self):

        self.blob_service = (
            BlobServiceClient.from_connection_string(
                V_CONNECTION_STRING
            )
        )

    # ==========================================
    # BUILD VERSION PATH
    # ==========================================
    def build_version_path(
        self,
        user_id,
        job_id,
        stage,
        dataset_name
    ):

        return (
            f"{user_id}/{job_id}/"
            f"{stage}/{dataset_name}"
        )

    # ==========================================
    # SAVE VERSION
    # ==========================================
    def save_version(
        self,
        df,
        user_id,
        job_id,
        stage,
        dataset_name
    ):

        path = self.build_version_path(
            user_id,
            job_id,
            stage,
            dataset_name
        )

        output = BytesIO()

        df.to_csv(output, index=False)

        output.seek(0)

        blob = self.blob_service.get_blob_client(
            container=V_DATASET_CONTAINER,
            blob=path
        )

        blob.upload_blob(
            output.getvalue(),
            overwrite=True
        )

        return path