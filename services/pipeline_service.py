# import uuid
# import json
# from datetime import datetime

# from azure.storage.blob import BlobServiceClient

# from app.agent.prompt_parser import parse_prompt
# from app.services.ingestion_service import IngestionService
# from app.services.datamodel_service import DataModelService
# from app.services.dataset_service import DatasetService
# from app.services.etl_service import ETLService

# from app.config import BLOB_CONN_STR


# class PipelineService:

#     def __init__(self):
#         self.ingestion = IngestionService()
#         self.datamodel = DataModelService()
#         self.dataset = DatasetService()
#         self.etl = ETLService()

#         self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

#         self.final_container = "finaldataset"
#         self.pipeline_container = "pipelines"
#         self.schedule_container = "schedules"

#     # =====================================================
#     # UPLOAD FILE
#     # =====================================================
#     def upload_to_blob(self, local_path, blob_path):

#         blob = self.blob_service.get_blob_client(
#             container=self.final_container,
#             blob=blob_path
#         )

#         with open(local_path, "rb") as f:
#             blob.upload_blob(f, overwrite=True)

#         return blob_path

#     # =====================================================
#     # SAVE PIPELINE
#     # =====================================================
#     def save_pipeline(self, user_id, pipeline_name, prompt, sources):

#         container = self.blob_service.get_container_client(self.pipeline_container)

#         try:
#             container.create_container()
#         except:
#             pass

#         data = {
#             "pipeline_name": pipeline_name,
#             "user_id": user_id,
#             "prompt": prompt,
#             "sources": sources,
#             "created_at": datetime.utcnow().isoformat()
#         }

#         path = f"{user_id}/{pipeline_name}/pipeline.json"

#         blob = container.get_blob_client(path)
#         blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

#         return path

#     # =====================================================
#     # SAVE SCHEDULE
#     # =====================================================
#     def save_schedule(self, user_id, pipeline_name, cron):

#         container = self.blob_service.get_container_client(self.schedule_container)

#         try:
#             container.create_container()
#         except:
#             pass

#         data = {
#             "pipeline_name": pipeline_name,
#             "user_id": user_id,
#             "cron": cron,
#             "enabled": True,
#             "created_at": datetime.utcnow().isoformat()
#         }

#         path = f"{user_id}/{pipeline_name}/schedule.json"

#         blob = container.get_blob_client(path)
#         blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

#         return path

#     # =====================================================
#     # RUN PIPELINE
#     # =====================================================
#     def run(self, prompt, user_id, job_id=None):

#         if not job_id:
#             job_id = str(uuid.uuid4())

#         print("\n🚀 PIPELINE STARTED")

#         # -------------------------------
#         # Step 1: Parse Prompt
#         # -------------------------------
#         plan = parse_prompt(prompt)
#         sources = plan.get("sources", [])

#         if not sources:
#             raise Exception("No sources detected")

#         # -------------------------------
#         # Step 2: Ingestion
#         # -------------------------------
#         uploaded = self.ingestion.ingest_sources(
#             sources=sources,
#             user_id=user_id,
#             job_id=job_id
#         )

#         # -------------------------------
#         # Step 3: Data Modeling (optional)
#         # -------------------------------
#         model_output = self.datamodel.execute(
#             user_id=user_id,
#             job_id=job_id
#         )

#         # -------------------------------
#         # Step 4: Dataset
#         # -------------------------------
#         dataset_output = self.dataset.execute(
#             model_output=model_output,
#             user_id=user_id,
#             job_id=job_id
#         )

#         # -------------------------------
#         # Step 5: ETL
#         # -------------------------------
#         etl_output = self.etl.execute(
#             dataset=dataset_output["dataset"],
#             user_id=user_id,
#             job_id=job_id,
#             model_output=model_output
#         )

#         # -------------------------------
#         # Step 6: Save Final Dataset
#         # -------------------------------
#         final_blob_path = f"{user_id}/{job_id}/final_dataset.csv"

#         self.upload_to_blob(
#             local_path=etl_output["file"],
#             blob_path=final_blob_path
#         )

#         # =====================================================
#         # 🔥 FINAL DYNAMIC DATA MODEL (FIXED)
#         # =====================================================

#         lineage = etl_output.get("lineage") or {}
# # =====================================================
# # ✅ FIXED FINAL DATA MODEL (AI-FIRST + VALIDATION)
# # =====================================================

#         etl_columns = etl_output.get("columns", [])
#         preview = etl_output.get("preview", [])
#         total_rows = etl_output.get("rows", 0)

# # =====================================================
# # ✅ FINAL CORRECT OUTPUT (NO UNDEFINED VARS)
# # =====================================================

#         model_data = model_output["model_output"]["data"]

#         # -------------------------------
#         # FACT TABLE (from logs / AI)
#         # -------------------------------
#         fact_table = model_data["model"].get("fact_table")

#         # fallback (if missing)
#         if not fact_table:
#             fact_table = "order_items"   # or safe fallback logic

#         # -------------------------------
#         # DIMENSIONS (from relationships)
#         # -------------------------------
#         dimension_tables = list({
#             rel["to_table"]
#             for rel in model_data.get("relationships", [])
#             if rel["from_table"] == fact_table
#         })

#         # fallback (if empty)
#         if not dimension_tables:
#             dimension_tables = [
#                 t["table_name"]
#                 for t in model_data["tables"]
#                 if t["table_name"] != fact_table
#             ]

#         # -------------------------------
#         # RELATIONSHIPS (real joins)
#         # -------------------------------
#         relationships = [
#             {
#                 "from": rel["from_table"],
#                 "to": rel["to_table"],
#                 "join": f"{rel['from_column']} = {rel['to_column']}"
#             }
#             for rel in model_data.get("relationships", [])
#         ]

#         # -------------------------------
#         # FINAL DATASET (from ETL)
#         # -------------------------------
#         final_dataset = {
#             "rows": etl_output.get("rows"),
#             "columns": etl_output.get("columns"),
#             "preview": etl_output.get("preview")
#         }

#         # -------------------------------
#         # FINAL RESPONSE
#         # -------------------------------
#         return {
#             "status": "success",

#             "data_model": {
#                 "fact_table": fact_table,
#                 "dimension_tables": dimension_tables
#             },

#             "relationships": relationships,

#             "schemas": model_data.get("schemas", {}),

#             "final_dataset": final_dataset
#         }

#     # =====================================================
#     # SAVE PIPELINE
#     # =====================================================
#     def create_pipeline(self, user_id, pipeline_name, prompt, sources):
#         return self.save_pipeline(user_id, pipeline_name, prompt, sources)

#     # =====================================================
#     # SCHEDULE PIPELINE
#     # =====================================================
#     def schedule_pipeline(self, user_id, pipeline_name, cron):
#         return self.save_schedule(user_id, pipeline_name, cron)

#     # =====================================================
#     # RUN SAVED PIPELINE
#     # =====================================================
#     def run_saved_pipeline(self, user_id, pipeline_name):

#         path = f"{user_id}/{pipeline_name}/pipeline.json"

#         blob = self.blob_service.get_blob_client(self.pipeline_container, path)
#         data = json.loads(blob.download_blob().readall())

#         return self.run(
#             prompt=data["prompt"],
#             user_id=user_id
#         )


import uuid
import json
from azure.storage.blob import BlobServiceClient

from app.agent.prompt_parser import parse_prompt
from app.services.ingestion_service import IngestionService
from app.services.datamodel_service import DataModelService
from app.services.dataset_service import DatasetService
from app.services.etl_service import ETLService


from app.config import BLOB_CONN_STR


class PipelineService:

    def __init__(self):
        self.ingestion = IngestionService()
        self.datamodel = DataModelService()
        self.dataset = DatasetService()
        self.etl = ETLService()

        # ✅ KEEP THIS (needed by pipeline_manager)
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    # =====================================================
    # RUN PIPELINE
    # =====================================================
    def run(self, prompt, user_id, job_id=None):

        if not job_id:
            job_id = str(uuid.uuid4())

        print("\n🚀 PIPELINE STARTED")

        # =========================
        # STEP 1: PARSE
        # =========================
        plan = parse_prompt(prompt)
        sources = plan.get("sources", [])

        if not sources:
            raise Exception("No sources detected")

        # =========================
        # STEP 2: INGESTION
        # =========================
        self.ingestion.ingest_sources(
            sources=sources,
            user_id=user_id,
            job_id=job_id
        )

        # =========================
        # STEP 3: DATA MODEL
        # =========================
        model_output = self.datamodel.execute(
            user_id=user_id,
            job_id=job_id
        )

        # =========================
        # STEP 4: DATASET
        # =========================
        dataset_output = self.dataset.execute(
            model_output=model_output,
            user_id=user_id,
            job_id=job_id
        )

        # =========================
        # STEP 5: ETL (🔥 FIXED)
        # =========================
        etl_output = self.etl.execute(
            dataset=dataset_output["dataset"],
            user_id=user_id,
            job_id=job_id,
            model_output=model_output
        )

        # ❌ REMOVED:
        # upload_to_blob()
        # etl_output["file"]

        # =========================
        # FINAL MODEL
        # =========================
        model_data = model_output["model_output"]["data"]

        # FACT TABLE
        fact_table = model_data.get("fact_override") or model_data["model"].get("fact_table")

        # =========================
        # DIMENSIONS (dynamic)
        # =========================
        dimension_tables = set()

        for rel in model_data.get("relationships", []):
            if rel["from_table"] == fact_table:
                dimension_tables.add(rel["to_table"])

        for rel in model_data.get("relationships", []):
            if rel["from_table"] in dimension_tables:
                dimension_tables.add(rel["to_table"])

        dimension_tables = list(dimension_tables)

        # =========================
        # RELATIONSHIPS
        # =========================
        relationships = [
            {
                "from": rel["from_table"],
                "to": rel["to_table"],
                "join": f"{rel['from_column']} = {rel['to_column']}"
            }
            for rel in model_data.get("relationships", [])
        ]

        # =========================
        # FINAL DATASET
        # =========================
        final_dataset = {
            "rows": etl_output.get("rows"),
            "columns": etl_output.get("columns"),
            "preview": etl_output.get("preview")
        }


        # =========================
        # SAVE RESULT TO BLOB
        # =========================
        container_name = "jobs"
        blob_path = f"{user_id}/{job_id}/result.json"

        result_payload = {
            "status": "success",
            "data_model": {
                "fact_table": fact_table,
                "dimension_tables": dimension_tables
            },
            "relationships": relationships,
            "schemas": model_data.get("schemas", {}),
            "final_dataset": final_dataset,
            "pipeline_metadata": {
                "prompt": prompt,
                "user_id": user_id,
                "job_id": job_id
            }
        }

        blob_client = self.blob_service.get_blob_client(
            container=container_name,
            blob=blob_path
        )

        blob_client.upload_blob(
            json.dumps(result_payload),
            overwrite=True
        )

        print(f"✅ Saved result to: {blob_path}")

        return {
            "status": "success",
            "data_model": {
                "fact_table": fact_table,
                "dimension_tables": dimension_tables
            },
            "relationships": relationships,
            "schemas": model_data.get("schemas", {}),
            "final_dataset": final_dataset,

            # used by main.py
            "pipeline_metadata": {
                "prompt": prompt,
                "user_id": user_id,
                "job_id": job_id
            }
        }