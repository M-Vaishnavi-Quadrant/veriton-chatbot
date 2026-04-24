# import boto3
# from azure.storage.blob import BlobServiceClient
# from app.config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER, AZURE_CONTAINER


# class IngestionService:

#     def __init__(self):
#         # Azure Blob client
#         self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

#         # S3 client (uses env creds automatically)
#         self.s3 = boto3.client("s3")

#     # ==========================
#     # Upload to Azure Blob
#     # ==========================
#     def upload_to_blob(self, data, filename, user_id, job_id):

#         blob_path = f"{user_id}/{job_id}/{filename}"

#         print(f"📤 Uploading to Blob → {blob_path}")

#         blob_client = self.blob_service.get_blob_client(
#             container=DATA_INGESTION_CONTAINER,
#             blob=blob_path
#         )

#         blob_client.upload_blob(data, overwrite=True)

#         return blob_path

#     # ==========================
#     # S3 INGESTION (UPDATED)
#     # ==========================
#     def ingest_from_s3(self, bucket, file_name, user_id, job_id):

#         try:
#             print(f"📥 Fetching from S3 → {bucket}/{file_name}")

#             obj = self.s3.get_object(
#                 Bucket=bucket,
#                 Key=file_name
#             )

#             data = obj["Body"].read()

#             return self.upload_to_blob(data, file_name, user_id, job_id)

#         except Exception as e:
#             raise Exception(f"S3 ingestion failed: {str(e)}")

#     # ==========================
#     # AZURE INGESTION (INTERNAL COPY)
#     # ==========================
#     def ingest_from_azure(self, file_name, user_id, job_id):

#         try:
#             print(f"📥 Fetching from Azure Blob → {file_name}")

#             blob_client = self.blob_service.get_blob_client(
#                 container=AZURE_CONTAINER,
#                 blob=file_name
#             )

#             data = blob_client.download_blob().readall()

#             return self.upload_to_blob(data, file_name, user_id, job_id)

#         except Exception as e:
#             raise Exception(f"Azure ingestion failed: {str(e)}")

#     # ==========================
#     # MAIN INGESTION
#     # ==========================
#     def ingest_sources(self, sources, user_id, job_id):

#         uploaded = []

#         for src in sources:

#             src_type = src.get("type")

#             # -------- S3 --------
#             if src_type == "s3":

#                 bucket = src.get("bucket")
#                 file_name = src.get("file_name")

#                 if not bucket or not file_name:
#                     raise Exception("❌ Missing bucket or file_name for S3")

#                 path = self.ingest_from_s3(
#                     bucket=bucket,
#                     file_name=file_name,
#                     user_id=user_id,
#                     job_id=job_id
#                 )

#             # -------- AZURE --------
#             elif src_type == "azure":

#                 file_name = src.get("file_name")

#                 if not file_name:
#                     raise Exception("❌ Missing file_name for Azure source")

#                 path = self.ingest_from_azure(
#                     file_name=file_name,
#                     user_id=user_id,
#                     job_id=job_id
#                 )

#             else:
#                 raise Exception(f"❌ Unsupported source: {src_type}")

#             uploaded.append(path)

#         print(f"✅ Uploaded files: {uploaded}")

#         return uploaded

import boto3
import os
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER, AZURE_CONTAINER


class IngestionService:

    def __init__(self):
        # Azure Blob client
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

        # S3 client
        self.s3 = boto3.client("s3")

    # =========================================================
    # NORMALIZE FILE NAME (🔥 KEY FIX)
    # =========================================================
    def normalize_filename(self, filename: str) -> str:
        return os.path.basename(filename).lower()

    # =========================================================
    # UPLOAD TO AZURE BLOB
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
    # S3 INGESTION (ROBUST + NORMALIZED)
    # =========================================================
    def ingest_from_s3(self, bucket, file_name, user_id, job_id):

        try:
            print(f"📥 Fetching from S3 → {bucket}/{file_name}")

            # Normalize incoming filename
            file_name = self.normalize_filename(file_name)

            # -------- STEP 1: Direct fetch --------
            try:
                obj = self.s3.get_object(
                    Bucket=bucket,
                    Key=file_name
                )

                print("✅ Direct match found")

                data = obj["Body"].read()
                return self.upload_to_blob(data, file_name, user_id, job_id)

            except self.s3.exceptions.NoSuchKey:
                print("⚠️ Direct match failed, searching...")

            # -------- STEP 2: List & search --------
            response = self.s3.list_objects_v2(Bucket=bucket)

            if "Contents" not in response:
                raise Exception(f"Bucket '{bucket}' is empty or not accessible")

            # -------- STEP 2A: Exact match ignoring case --------
            for item in response["Contents"]:
                key = item["Key"]

                if key.lower() == file_name.lower():
                    print(f"✅ Found exact match (case-insensitive) → {key}")

                    obj = self.s3.get_object(Bucket=bucket, Key=key)
                    data = obj["Body"].read()

                    actual_name = key.split("/")[-1]
                    return self.upload_to_blob(data, actual_name, user_id, job_id)

            # -------- STEP 2B: Partial match --------
            for item in response["Contents"]:
                key = item["Key"]

                if file_name.lower() in key.lower():
                    print(f"✅ Found partial match → {key}")

                    obj = self.s3.get_object(Bucket=bucket, Key=key)
                    data = obj["Body"].read()

                    actual_name = key.split("/")[-1]
                    return self.upload_to_blob(data, actual_name, user_id, job_id)

            # -------- NOT FOUND --------
            raise Exception(f"❌ File '{file_name}' not found in bucket '{bucket}'")

        except Exception as e:
            raise Exception(f"S3 ingestion failed: {str(e)}")

    # =========================================================
    # AZURE INGESTION (NORMALIZED)
    # =========================================================
    def ingest_from_azure(self, file_name, user_id, job_id):

        try:
            print(f"📥 Fetching from Azure Blob → {file_name}")

            blob_client = self.blob_service.get_blob_client(
                container=AZURE_CONTAINER,
                blob=file_name
            )

            data = blob_client.download_blob().readall()

            # Normalize before storing
            clean_name = self.normalize_filename(file_name)

            return self.upload_to_blob(data, clean_name, user_id, job_id)

        except Exception as e:
            raise Exception(f"Azure ingestion failed: {str(e)}")

    # =========================================================
    # MAIN INGESTION
    # =========================================================
    def ingest_sources(self, sources, user_id, job_id):

        uploaded = []

        for src in sources:

            src_type = src.get("type")

            # -------- S3 --------
            if src_type == "s3":

                bucket = src.get("bucket")
                file_name = src.get("file_name")

                if not bucket or not file_name:
                    raise Exception("❌ Missing bucket or file_name for S3")

                print(f"🧾 Parsed S3 → bucket={bucket}, file={file_name}")

                path = self.ingest_from_s3(
                    bucket=bucket,
                    file_name=file_name,
                    user_id=user_id,
                    job_id=job_id
                )

            # -------- AZURE --------
            elif src_type == "azure":

                file_name = src.get("file_name")

                if not file_name:
                    raise Exception("❌ Missing file_name for Azure source")

                path = self.ingest_from_azure(
                    file_name=file_name,
                    user_id=user_id,
                    job_id=job_id
                )

            else:
                raise Exception(f"❌ Unsupported source: {src_type}")

            uploaded.append(path)

        print(f"✅ Uploaded files: {uploaded}")

        return uploaded
