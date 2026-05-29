import boto3
import os
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER


class IngestionService:

    def __init__(self):
        # Azure Blob client
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

        # S3 client (uses env credentials)
        self.s3 = boto3.client("s3")

    # =========================================================
    # NORMALIZE FILE NAME
    # =========================================================
    def normalize_filename(self, filename: str) -> str:
        return os.path.basename(filename).lower()

    # =========================================================
    # UPLOAD TO AZURE BLOB (INGESTION CONTAINER)
    # =========================================================
    def upload_to_blob(self, data, filename, user_id, job_id):

        clean_name = self.normalize_filename(filename)
        blob_path = f"{user_id}/{job_id}/{clean_name}"

        print(f"📤 Uploading to Blob → {blob_path}")

        blob_client = self.blob_service.get_blob_client(
            container=DATA_INGESTION_CONTAINER,
            blob=blob_path
        )

        blob_client.upload_blob(data, overwrite=True)

        return blob_path

    # =========================================================
    # S3 INGESTION
    # =========================================================
    def ingest_from_s3(self, bucket, file_name, user_id, job_id):

        try:
            print(f"📥 Fetching from S3 → {bucket}/{file_name}")

            if not bucket:
                raise Exception("❌ bucket is required for S3 source")

            if not file_name:
                raise Exception("❌ file_name is required for S3 source")

            # Normalize filename
            clean_name = self.normalize_filename(file_name)

            # Fetch from S3
            obj = self.s3.get_object(
                Bucket=bucket,
                Key=file_name
            )

            data = obj["Body"].read()

            # Upload to ingestion container
            return self.upload_to_blob(data, clean_name, user_id, job_id)

        except Exception as e:
            raise Exception(f"S3 ingestion failed: {str(e)}")

    # =========================================================
    # AZURE INGESTION (DYNAMIC CONTAINER)
    # =========================================================
    def ingest_from_azure(self, container_name, file_name, user_id, job_id):

        try:
            print(f"📥 Fetching from Azure Blob → {container_name}/{file_name}")

            if not container_name:
                raise Exception("❌ container_name is required for Azure source")

            if not file_name:
                raise Exception("❌ file_name is required for Azure source")

            # Normalize filename
            clean_name = self.normalize_filename(file_name)

            blob_client = self.blob_service.get_blob_client(
                container=container_name,   # ✅ dynamic container
                blob=file_name              # supports folders
            )

            if not blob_client.exists():
                raise Exception(f"❌ File not found: {container_name}/{file_name}")

            data = blob_client.download_blob().readall()

            # Upload into ingestion container
            return self.upload_to_blob(data, clean_name, user_id, job_id)

        except Exception as e:
            raise Exception(f"Azure ingestion failed: {str(e)}")

    # =========================================================
    # MAIN INGESTION ENTRY
    # =========================================================
    def ingest_sources(self, sources, user_id, job_id):

        uploaded_paths = []

        for src in sources:

            src_type = src.get("type")

            # ---------------- S3 ----------------
            if src_type == "s3":

                bucket = src.get("bucket")
                file_name = src.get("file_name")

                path = self.ingest_from_s3(
                    bucket=bucket,
                    file_name=file_name,
                    user_id=user_id,
                    job_id=job_id
                )

            # ---------------- AZURE ----------------
            elif src_type == "azure":

                container_name = src.get("container")
                file_name = src.get("file_name")

                path = self.ingest_from_azure(
                    container_name=container_name,
                    file_name=file_name,
                    user_id=user_id,
                    job_id=job_id
                )

            # ---------------- UPLOAD (handled externally) ----------------
            elif src_type == "upload":

                data = src.get("data")
                file_name = src.get("file_name")

                if not data or not file_name:
                    raise Exception("❌ upload source requires data and file_name")

                path = self.upload_to_blob(
                    data=data,
                    filename=file_name,
                    user_id=user_id,
                    job_id=job_id
                )

            else:
                raise Exception(f"❌ Unsupported source type: {src_type}")

            uploaded_paths.append(path)

        print(f"✅ Uploaded files: {uploaded_paths}")

        return uploaded_paths
