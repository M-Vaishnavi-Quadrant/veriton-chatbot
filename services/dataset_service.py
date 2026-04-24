# import json
# import logging
# import re
# from azure.storage.blob import BlobServiceClient
# from openai import AzureOpenAI

# from app.config import (
#     BLOB_CONN_STR,
#     AI_API_KEY,
#     AI_ENDPOINT,
#     AI_MODEL,
#     DATASET_CONTAINER
# )

# logging.basicConfig(level=logging.INFO)


# class DatasetService:

#     def __init__(self):
#         self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

#         self.client = AzureOpenAI(
#             api_key=AI_API_KEY,
#             azure_endpoint=AI_ENDPOINT,
#             api_version="2024-05-01-preview"
#         )

#     # =========================================================
#     # SAFE PARSE
#     # =========================================================
#     def safe_parse(self, text):
#         try:
#             return json.loads(text)
#         except:
#             text = re.sub(r",\s*}", "}", text)
#             text = re.sub(r",\s*]", "]", text)
#             return json.loads(text)

#     # =========================================================
#     # AI DATASET GENERATION
#     # =========================================================
#     def generate_dataset(self, model_data):

#         response = self.client.chat.completions.create(
#             model=AI_MODEL,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": """
# Return ONLY valid JSON.

# Rules:
# - Use fact_table as base
# - Use ONLY columns from model_data
# - Include join keys
# - Avoid duplicates

# FORMAT:
# {
#   "dataset_name": "dynamic_dataset",
#   "columns": [{"name": "...", "source_table": "..."}],
#   "joins": []
# }
# """
#                 },
#                 {"role": "user", "content": json.dumps(model_data)}
#             ],
#             temperature=0
#         )

#         raw = response.choices[0].message.content
#         return self.safe_parse(raw)

#     # =========================================================
#     # VALID TABLES
#     # =========================================================
#     def get_valid_tables(self, model_data):

#         valid = set()

#         for t in model_data["tables"]:
#             name = t["table_name"]
#             ttype = t.get("table_type")
#             derived = t.get("derived_from")

#             if ttype in ["SOURCE", "DIM"]:
#                 if derived and derived != name:
#                     continue
#                 valid.add(name)

#         return valid

#     # =========================================================
#     # 🔥 NORMALIZE COLUMN NAME (CRITICAL)
#     # =========================================================
#     def normalize_name(self, name):
#         name = name.strip().lower()
#         name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
#         name = name.replace(" ", "_")
#         return name

#     # =========================================================
#     # 🔥 SEMANTIC BASE (CRITICAL)
#     # =========================================================
#     def get_base_name(self, name):
#         name = self.normalize_name(name)

#         # remove common suffixes dynamically
#         tokens = name.split("_")

#         if len(tokens) > 1:
#             return tokens[-1]  # last token = semantic base

#         return name

#     # =========================================================
#     # 🔥 FULL DEDUP FIX (FINAL)
#     # =========================================================
#     def deduplicate_dataset(self, dataset, valid_tables):

#         seen_base = set()
#         cleaned = []

#         for col in dataset["columns"]:

#             table = col["source_table"]
#             name = col["name"]

#             # skip invalid tables
#             if table not in valid_tables:
#                 continue

#             normalized = self.normalize_name(name)
#             base = self.get_base_name(normalized)

#             if base in seen_base:
#                 continue

#             seen_base.add(base)

#             col["name"] = normalized
#             cleaned.append(col)

#         dataset["columns"] = cleaned
#         return dataset

#     # =========================================================
#     # ENRICH DATASET
#     # =========================================================
#     def enrich_dataset(self, dataset, model_data, valid_tables):

#         existing = {(c["name"], c["source_table"]) for c in dataset["columns"]}

#         for table in model_data.get("tables", []):

#             table_name = table["table_name"]

#             if table_name not in valid_tables:
#                 continue

#             for col in table.get("columns", []):

#                 name = col["name"]
#                 key = (name, table_name)

#                 if key in existing:
#                     continue

#                 if (
#                     col.get("is_foreign_key")
#                     or col.get("is_primary_key")
#                     or col.get("distinct_count", 100) < 50
#                 ):
#                     dataset["columns"].append({
#                         "name": name,
#                         "source_table": table_name
#                     })

#         return dataset

#     # =========================================================
#     # CLEAN ALIAS (NO COLLISION PATCHING)
#     # =========================================================
#     def apply_alias(self, dataset):

#         seen = set()

#         for col in dataset["columns"]:

#             base = f"{col['source_table']}_{col['name']}".lower()

#             if base in seen:
#                 continue  # skip duplicates instead of renaming

#             col["alias"] = base
#             seen.add(base)

#         # remove skipped cols
#         dataset["columns"] = [c for c in dataset["columns"] if "alias" in c]

#         return dataset

#     # =========================================================
#     # VALIDATE
#     # =========================================================
#     def validate_dataset(self, dataset):

#         if not dataset.get("columns"):
#             raise Exception("❌ Dataset has no columns")

#     # =========================================================
#     # SAVE
#     # =========================================================
#     def save_dataset(self, dataset, user_id, job_id):

#         container = self.blob_service.get_container_client(DATASET_CONTAINER)

#         try:
#             container.create_container()
#         except:
#             pass

#         path = f"{user_id}/{job_id}/dataset.json"

#         blob = container.get_blob_client(path)
#         blob.upload_blob(json.dumps(dataset, indent=2), overwrite=True)

#         return path

#     # =========================================================
#     # EXECUTE
#     # =========================================================
#     def execute(self, model_output, user_id, job_id):

#         model_data = model_output["model_output"]["data"]

#         # 1. AI dataset
#         dataset = self.generate_dataset(model_data)

#         # 2. valid tables
#         valid_tables = self.get_valid_tables(model_data)

#         # 3. enrich
#         dataset = self.enrich_dataset(dataset, model_data, valid_tables)

#         # 4. 🔥 FINAL DEDUP FIX
#         dataset = self.deduplicate_dataset(dataset, valid_tables)

#         # 5. alias
#         dataset = self.apply_alias(dataset)

#         # 6. validate
#         self.validate_dataset(dataset)

#         # 7. save
#         blob_path = self.save_dataset(dataset, user_id, job_id)

#         return {
#             "dataset": dataset,
#             "blob_path": blob_path
#         }

import json
from azure.storage.blob import BlobServiceClient

from config import BLOB_CONN_STR, DATASET_CONTAINER


class DatasetService:

    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    # =====================================================
    # LOGGER
    # =====================================================
    def log(self, msg):
        print(f"[DATASET] {msg}")

    # =====================================================
    # BUILD OPTIMIZED DATASET
    # =====================================================
    def build_dataset(self, model_data):

        schemas = model_data.get("schemas", {})
        relationships = model_data.get("relationships", [])
        fact_table = model_data["model"]["fact_table"]

        dataset_columns = []
        seen = set()

        self.log(f"Fact table: {fact_table}")

        # =============================
        # FACT → KEEP ALL
        # =============================
        for col in schemas.get(fact_table, []):
            dataset_columns.append({
                "name": col,
                "source_table": fact_table,
                "alias": col
            })
            seen.add((fact_table, col))

        # =============================
        # DIMENSIONS → DESCRIPTIVE ONLY
        # =============================
        for table, cols in schemas.items():

            if table == fact_table:
                continue

            for col in cols:

                # ❌ Skip IDs
                if col.endswith("_id"):
                    continue

                # ❌ Skip numeric-heavy metrics
                if col.lower() in [
                    "price", "amount", "total_amount",
                    "subtotal", "quantity"
                ]:
                    continue

                key = (table, col)

                if key in seen:
                    continue

                dataset_columns.append({
                    "name": col,
                    "source_table": table,
                    "alias": f"{table}_{col}".lower()
                })

                seen.add(key)

        if not dataset_columns:
            raise Exception("❌ Dataset has no columns")

        self.log(f"Final dataset columns: {len(dataset_columns)}")

        return {
            "dataset_name": "optimized_dataset",
            "columns": dataset_columns,
            "joins": relationships
        }

    # =====================================================
    # SAVE
    # =====================================================
    def save_dataset(self, dataset, user_id, job_id):

        container = self.blob_service.get_container_client(DATASET_CONTAINER)

        try:
            container.create_container()
        except:
            pass

        path = f"{user_id}/{job_id}/dataset.json"

        blob = container.get_blob_client(path)
        blob.upload_blob(json.dumps(dataset, indent=2), overwrite=True)

        return path

    # =====================================================
    # EXECUTE
    # =====================================================
    def execute(self, model_output, user_id, job_id):

        model_data = model_output["model_output"]["data"]

        dataset = self.build_dataset(model_data)

        blob_path = self.save_dataset(dataset, user_id, job_id)

        return {
            "dataset": dataset,
            "blob_path": blob_path
        }
