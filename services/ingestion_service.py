import boto3
import os
import re
import io
import pandas as pd
from hdbcli import dbapi
from azure.storage.blob import BlobServiceClient
from config import (
    BLOB_CONN_STR,
    DATA_INGESTION_CONTAINER,
    HANA_HOST,
    HANA_PORT,
    HANA_USER,
    HANA_PASSWORD,
)

# Only allow safe SQL identifier characters (letters, digits, underscore).
# SAP HANA doesn't support parameterizing identifiers (schema/table names)
# the way it does values, so we validate manually before ever putting
# user-supplied schema/table strings into a query.
_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_]+$')


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
    # SAP HANA — CONNECTION HELPER
    # =========================================================
    def _get_hana_connection(self):
        return dbapi.connect(
            address=HANA_HOST,
            port=HANA_PORT,
            user=HANA_USER,
            password=HANA_PASSWORD,
        )

    # =========================================================
    # SAP HANA — VALIDATE IDENTIFIER (PREVENT SQL INJECTION)
    # =========================================================
    def _validate_identifier(self, name, label):
        if not name or not _IDENTIFIER_RE.match(name):
            raise Exception(
                f"❌ Invalid {label}: '{name}' "
                f"(only letters, digits, and underscore are allowed)"
            )

    # =========================================================
    # SAP HANA INGESTION
    # =========================================================
    def ingest_from_sap_hana(self, schema, table, user_id, job_id):

        try:
            print(f"📥 Fetching from SAP HANA → {schema}.{table}")

            if not schema:
                raise Exception("❌ schema is required for SAP HANA source")

            if not table:
                raise Exception("❌ table is required for SAP HANA source")

            # Reject anything that isn't a plain identifier before it
            # ever touches a SQL string.
            self._validate_identifier(schema, "schema")
            self._validate_identifier(table, "table")

            schema_upper = schema.upper()
            table_upper = table.upper()

            conn = self._get_hana_connection()

            try:
                cursor = conn.cursor()

                # ---- resolve actual, case-preserved schema name ----
                # HANA auto-uppercases unquoted identifiers, but a table
                # or schema created with a quoted identifier (e.g.
                # CREATE TABLE "Employees" (...)) keeps its exact case.
                # Compare case-insensitively, then use the *real* stored
                # name (whatever case it actually is) going forward.
                cursor.execute(
                    "SELECT SCHEMA_NAME FROM SYS.SCHEMAS "
                    "WHERE UPPER(SCHEMA_NAME) = UPPER(?)",
                    (schema,)
                )
                schema_row = cursor.fetchone()
                if not schema_row:
                    raise Exception(f"❌ Schema not found: {schema}")

                actual_schema = schema_row[0]

                # ---- resolve actual, case-preserved table name ----
                cursor.execute(
                    "SELECT TABLE_NAME FROM SYS.TABLES "
                    "WHERE SCHEMA_NAME = ? AND UPPER(TABLE_NAME) = UPPER(?)",
                    (actual_schema, table)
                )
                table_row = cursor.fetchone()
                if not table_row:
                    raise Exception(
                        f"❌ Table not found: {schema}.{table}"
                    )

                actual_table = table_row[0]

                if actual_schema != schema or actual_table != table:
                    print(
                        f"🔧 Case mismatch corrected: "
                        f"'{schema}.{table}' → '{actual_schema}.{actual_table}'"
                    )

                # Values came straight from the catalog (not user input),
                # and _validate_identifier already rejected anything with
                # quote/injection characters in the original input — safe
                # to interpolate as quoted identifiers.
                query = f'SELECT * FROM "{actual_schema}"."{actual_table}"'
                print(f"🔎 Running query: {query}")

                df = pd.read_sql(query, conn)

            finally:
                conn.close()

            print(f"✅ Retrieved {len(df)} row(s) from {actual_schema}.{actual_table}")

            # Convert to CSV bytes, same as every other source type, so
            # downstream (blob upload, datamodel, ETL) is unchanged.
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            data = csv_buffer.getvalue().encode("utf-8")

            clean_name = self.normalize_filename(f"{actual_table}.csv")

            return self.upload_to_blob(data, clean_name, user_id, job_id)

        except Exception as e:
            raise Exception(
                f"SAP HANA ingestion failed for schema='{schema}' table='{table}': {str(e)}"
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

            # ---------------- SAP HANA ----------------
            elif src_type == "sap_hana":

                schema = src.get("schema")
                table = src.get("table")

                path = self.ingest_from_sap_hana(
                    schema=schema,
                    table=table,
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

    # =========================================================
    # DEBUG HELPER — list what actually exists in a HANA schema
    # =========================================================
    def debug_list_hana_tables(self, schema):
        """
        Quick diagnostic: prints every table in `schema` so you can
        confirm the exact schema/table names (and their real casing)
        before ingesting.
        """
        self._validate_identifier(schema, "schema")

        conn = self._get_hana_connection()

        try:
            cursor = conn.cursor()

            # Resolve actual case-preserved schema name first
            cursor.execute(
                "SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?)",
                (schema,)
            )
            schema_row = cursor.fetchone()
            if not schema_row:
                print(f"⚠️ Schema not found: {schema}")
                return []

            actual_schema = schema_row[0]

            cursor.execute(
                "SELECT TABLE_NAME FROM SYS.TABLES "
                "WHERE SCHEMA_NAME = ? ORDER BY TABLE_NAME",
                (actual_schema,)
            )
            tables = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

        print(f"🔎 Found {len(tables)} table(s) in schema '{actual_schema}':")
        for t in tables:
            print(f"   - {repr(t)}")

        return tables
