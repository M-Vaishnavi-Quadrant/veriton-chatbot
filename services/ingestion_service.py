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
    # RESOLVE ACTUAL S3 KEY (CASE-INSENSITIVE SAFETY NET)
    # =========================================================
    def resolve_actual_key(self, bucket, file_name):
        """
        S3 keys are case-sensitive, but upstream callers (e.g. prompt
        parsing) may pass a different case than what's actually stored.
        This looks up the bucket contents and matches case-insensitively
        so a mismatch like 'calendar.csv' vs 'Calendar.csv' doesn't fail.

        Falls back to the original file_name if no case-insensitive match
        is found, so the resulting error still clearly reports NoSuchKey.
        """
        try:
            resp = self.s3.list_objects_v2(Bucket=bucket)
        except Exception as list_err:
            # Surface *why* listing failed instead of hiding it — this is
            # commonly a region mismatch or missing s3:ListBucket permission.
            print(f"⚠️ resolve_actual_key: list_objects_v2 failed for bucket='{bucket}': {list_err}")
            return file_name

        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        print(f"🔎 resolve_actual_key: bucket='{bucket}' contains {len(keys)} object(s): {keys}")

        for key in keys:
            if key.lower() == file_name.lower():
                return key

        print(f"⚠️ resolve_actual_key: no case-insensitive match for '{file_name}' in bucket='{bucket}'")
        return file_name

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

            # Resolve the real, case-correct key before attempting the fetch
            actual_key = self.resolve_actual_key(bucket, file_name)

            if actual_key != file_name:
                print(f"🔧 Case mismatch corrected: '{file_name}' → '{actual_key}'")

            # Normalize filename (used for the destination blob name only —
            # the S3 lookup itself uses actual_key, the real, case-correct key)
            clean_name = self.normalize_filename(actual_key)

            # Fetch from S3
            obj = self.s3.get_object(
                Bucket=bucket,
                Key=actual_key
            )

            data = obj["Body"].read()

            # Upload to ingestion container
            return self.upload_to_blob(data, clean_name, user_id, job_id)

        except Exception as e:
            # Include bucket + key so failures are traceable when ingesting
            # multiple files in a single request.
            raise Exception(
                f"S3 ingestion failed for bucket='{bucket}' key='{file_name}': {str(e)}"
            )

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
            raise Exception(
                f"Azure ingestion failed for container='{container_name}' key='{file_name}': {str(e)}"
            )

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

    # =========================================================
    # DEBUG HELPER — list what actually exists in an S3 bucket
    # =========================================================
    def debug_list_s3_keys(self, bucket, prefix=""):
        """
        Quick diagnostic: prints every key in `bucket` (optionally filtered
        by `prefix`) so you can compare exact casing/paths against what
        your ingestion request is asking for.
        """
        resp = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]

        print(f"🔎 Found {len(keys)} object(s) in '{bucket}' (prefix='{prefix}'):")
        for k in keys:
            print(f"   - {repr(k)}")

        return keys
