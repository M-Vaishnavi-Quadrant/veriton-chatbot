# import json
# import logging
# from azure.storage.blob import BlobServiceClient
# import azure.functions as func

# from app.config import (
#     BLOB_CONN_STR,
#     DATA_INGESTION_CONTAINER,
#     USER_CONTAINER
# )

# from app.processJob import main as process_job_main

# logging.basicConfig(level=logging.INFO)


# class DataModelService:

#     def __init__(self):
#         self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

#     # ==========================
#     # VALIDATE INGESTION FILES
#     # ==========================
#     def validate_files_exist(self, user_id: str, job_id: str):

#         container = self.blob_service.get_container_client(DATA_INGESTION_CONTAINER)

#         prefix = f"{user_id}/{job_id}/"
#         blobs = list(container.list_blobs(name_starts_with=prefix))

#         if not blobs:
#             raise Exception(f"❌ No files found for {user_id}/{job_id}")

#         files = [
#             b.name for b in blobs
#             if b.name.endswith((".csv", ".json", ".parquet"))
#         ]

#         if not files:
#             raise Exception("❌ No supported files found")

#         logging.info(f"✅ Found files: {files}")
#         return files

#     # ==========================
#     # COPY TO USERDATA
#     # ==========================
#     def copy_to_userdata_container(self, user_id: str, job_id: str):

#         logging.info("📦 Copying files to userdata container...")

#         source_container = self.blob_service.get_container_client(DATA_INGESTION_CONTAINER)
#         target_container = self.blob_service.get_container_client(USER_CONTAINER)

#         prefix = f"{user_id}/{job_id}/"
#         blobs = source_container.list_blobs(name_starts_with=prefix)

#         copied_files = []

#         for blob in blobs:

#             if not blob.name.endswith((".csv", ".json", ".parquet")):
#                 continue

#             filename = blob.name.split("/")[-1]

#             logging.info(f"➡️ Copying: {filename}")

#             source_blob = source_container.get_blob_client(blob.name)
#             data = source_blob.download_blob().readall()

#             target_blob = target_container.get_blob_client(blob.name)
#             target_blob.upload_blob(data, overwrite=True)

#             copied_files.append(blob.name)

#         logging.info(f"✅ Copied files to userdata: {copied_files}")
#         return copied_files

#     # ==========================
#     # 🔥 FULL DYNAMIC CLEANER
#     # ==========================
#     def clean_model(self, model_data):

#         tables = model_data.get("tables", [])
#         relationships = model_data.get("relationships", [])
#         model = model_data.get("model", {})

#         # --------------------------------
#         # 1. VALID TABLE SET
#         # --------------------------------
#         valid_tables = {t["table_name"] for t in tables}

#         # remove unwanted virtual tables
#         invalid_tables = {"date_dimension", "fact_table"}

#         valid_tables = valid_tables - invalid_tables

#         # --------------------------------
#         # 2. FIX FACT TABLE
#         # --------------------------------
#         fact = model.get("fact_table")

#         if fact == "fact_table" or fact not in valid_tables:
#             # fallback: pick first table
#             if tables:
#                 fact = tables[0]["table_name"]

#         # --------------------------------
#         # 3. CLEAN RELATIONSHIPS
#         # --------------------------------
#         cleaned_relationships = []

#         for rel in relationships:

#             from_table = rel.get("from_table")
#             to_table = rel.get("to_table")

#             # replace placeholder
#             if from_table == "fact_table":
#                 from_table = fact

#             # ❌ remove invalid tables
#             if from_table not in valid_tables:
#                 logging.warning(f"⚠️ Dropping invalid FROM → {rel}")
#                 continue

#             if to_table not in valid_tables:
#                 logging.warning(f"⚠️ Dropping invalid TO → {rel}")
#                 continue

#             # ❌ remove virtual tables
#             if to_table in invalid_tables:
#                 logging.warning(f"⚠️ Removing virtual table → {rel}")
#                 continue

#             # update rel
#             rel["from_table"] = from_table

#             cleaned_relationships.append(rel)

#         # --------------------------------
#         # 4. CLEAN TABLES
#         # --------------------------------
#         cleaned_tables = [
#             t for t in tables
#             if t["table_name"] not in invalid_tables
#         ]

#         # --------------------------------
#         # 5. UPDATE MODEL
#         # --------------------------------
#         model_data["tables"] = cleaned_tables
#         model_data["relationships"] = cleaned_relationships

#         # fix fact table
#         model_data["model"]["fact_table"] = fact

#         return model_data

#     # ==========================
#     # RUN DATA MODELING
#     # ==========================
#     def run_data_modeling(self, user_id: str, job_id: str):

#         logging.info("🚀 Starting Data Modeling...")

#         body = {
#             "user_id": user_id,
#             "job_id": job_id,
#             "ai_only": False
#         }

#         req = func.HttpRequest(
#             method="POST",
#             body=json.dumps(body).encode(),
#             url="/api/processJob"
#         )

#         response = process_job_main(req)
#         result = json.loads(response.get_body())

#         # -------------------------------------
#         # SUCCESS CASE
#         # -------------------------------------
#         if response.status_code == 200:

#             logging.info("✅ Data modeling succeeded")

#             if "data" in result:
#                 result["data"] = self.clean_model(result["data"])

#             return result

#         # -------------------------------------
#         # FAILURE → AUTO RECOVERY
#         # -------------------------------------
#         logging.warning("⚠️ Modeling failed — attempting recovery")

#         if "data" in result:

#             cleaned = self.clean_model(result["data"])

#             logging.info("✅ Recovered model dynamically")

#             return {
#                 "status": "success",
#                 "data": cleaned,
#                 "recovered": True
#             }

#         # -------------------------------------
#         # HARD FAILURE
#         # -------------------------------------
#         logging.error(f"❌ Modeling failed completely: {result}")
#         raise Exception(result)

#     # ==========================
#     # FULL PIPELINE
#     # ==========================
#     def execute(self, user_id: str, job_id: str):

#         logging.info("=" * 60)
#         logging.info("🚀 DATA MODEL PIPELINE STARTED")
#         logging.info(f"USER: {user_id}")
#         logging.info(f"JOB : {job_id}")
#         logging.info("=" * 60)

#         files = self.validate_files_exist(user_id, job_id)

#         self.copy_to_userdata_container(user_id, job_id)

#         result = self.run_data_modeling(user_id, job_id)

#         return {
#             "status": "success",
#             "user_id": user_id,
#             "job_id": job_id,
#             "input_files": files,
#             "model_output": result
#         }


import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from app.config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER


class DataModelService:

    def __init__(self):
        self.blob = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    def log(self, msg):
        print(f"[MODEL] {msg}")

    def normalize(self, col):
        return col.lower().replace(" ", "_")

    # =========================
    # LOAD TABLES
    # =========================
    def load_tables(self, user_id, job_id):

        container = self.blob.get_container_client(DATA_INGESTION_CONTAINER)
        prefix = f"{user_id}/{job_id}/"

        tables = {}

        for blob in container.list_blobs(name_starts_with=prefix):

            if not blob.name.endswith(".csv"):
                continue

            name = blob.name.split("/")[-1].replace(".csv", "")

            data = container.get_blob_client(blob.name).download_blob().readall()

            tables[name] = pd.read_csv(io.BytesIO(data))

        return tables

    # =========================
    # RELATIONSHIP DETECTION (DYNAMIC)
    # =========================
    def detect_relationships(self, tables):

        relationships = []

        for t1, df1 in tables.items():
            for t2, df2 in tables.items():

                if t1 == t2:
                    continue

                for c1 in df1.columns:

                    col1 = self.normalize(c1)

                    # =========================
                    # RULE 1: FK NAMING MATCH
                    # =========================
                    if not col1.endswith("_id"):
                        continue

                    # =========================
                    # RULE 2: SAME COLUMN EXISTS
                    # =========================
                    for c2 in df2.columns:

                        col2 = self.normalize(c2)

                        if col1 != col2:
                            continue

                        try:
                            s1 = set(df1[c1].dropna())
                            s2 = set(df2[c2].dropna())

                            if not s1 or not s2:
                                continue

                            # =========================
                            # RULE 3: FK → PK BEHAVIOR
                            # =========================
                            uniq1 = len(s1) / len(df1)   # FK side (low uniqueness)
                            uniq2 = len(s2) / len(df2)   # PK side (high uniqueness)

                            overlap = len(s1 & s2) / len(s1)

                            if (
                                overlap > 0.2 and      # relaxed
                                uniq2 > uniq1          # PK side more unique
                            ):
                                relationships.append({
                                    "from_table": t1,
                                    "to_table": t2,
                                    "from_column": c1,
                                    "to_column": c2
                                })

                        except:
                            continue

        return relationships

    # =========================
    # CLEAN RELATIONSHIPS
    # =========================
    def clean_relationships(self, relationships, fact_table):

        cleaned = []
        seen = set()

        for rel in relationships:

            f = rel["from_table"]
            t = rel["to_table"]

            if f == t:
                continue

            key = tuple(sorted([f, t]))

            if key in seen:
                continue

            seen.add(key)
            cleaned.append(rel)

        final = []

        for rel in cleaned:
            if rel["from_table"] == fact_table:
                final.append(rel)

        level1 = {r["to_table"] for r in final}

        for rel in cleaned:
            if rel["from_table"] in level1:
                final.append(rel)

        return final

    # =========================
    # FACT DETECTION
    # =========================
    def detect_fact(self, relationships, tables):

        score = {}

        for rel in relationships:
            score[rel["from_table"]] = score.get(rel["from_table"], 0) + 1

        if score:
            return max(score, key=score.get)

        return list(tables.keys())[0]

    # =========================
    # EXECUTE
    # =========================
    def execute(self, user_id, job_id):

        tables = self.load_tables(user_id, job_id)

        relationships = self.detect_relationships(tables)

        fact = self.detect_fact(relationships, tables)

        relationships = self.clean_relationships(relationships, fact)

        schemas = {t: list(df.columns) for t, df in tables.items()}

        self.log(f"Fact: {fact}")
        self.log(f"Relationships: {len(relationships)}")

        return {
            "model_output": {
                "data": {
                    "model": {
                        "fact_table": fact,
                        "dimension_tables": [t for t in tables if t != fact]
                    },
                    "relationships": relationships,
                    "schemas": schemas,
                    "tables": [{"table_name": t} for t in tables]
                }
            }
        }