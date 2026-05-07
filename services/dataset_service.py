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
    def execute(self, model_output, user_id, job_id, dataset_name):

        model_data = model_output["model_output"]["data"]

        dataset = self.build_dataset(model_data)

        # ✅ Attach dataset metadata (🔥 FIX)
        dataset["dataset_name"] = dataset_name
        dataset["dataset_path"] = f"{user_id}/{job_id}/{dataset_name}"

        blob_path = self.save_dataset(dataset, user_id, job_id)

        return {
            "dataset": dataset,
            "blob_path": blob_path
        }
