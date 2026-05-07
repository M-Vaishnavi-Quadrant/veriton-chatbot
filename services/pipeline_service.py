import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient

from agent.prompt_parser import parse_prompt
from services.ingestion_service import IngestionService
from services.datamodel_service import DataModelService
from services.dataset_service import DatasetService
from services.etl_service import ETLService
from services.onelake_service import OneLakeService

from config import BLOB_CONN_STR, V_CONNECTION_STRING, V_DATASET_CONTAINER


class PipelineService:

    def __init__(self):
        self.ingestion = IngestionService()
        self.datamodel = DataModelService()
        self.dataset = DatasetService()
        self.etl = ETLService()
        self.onelake = OneLakeService()

        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        self.dataset_blob_service = BlobServiceClient.from_connection_string(V_CONNECTION_STRING)

    # =========================
    # DATASET UPLOAD
    # =========================
    def upload_dataset(self, file_buffer, user_id, job_id, dataset_context):

        base_name = dataset_context.lower().replace(" ", "_")

        # 🔥 FIX: unique filename
        # timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        dataset_name = f"{base_name}_dataset.csv"
        blob_path = f"{user_id}/{job_id}/{dataset_name}"

        print("Uploading dataset to:", blob_path)

        blob = self.dataset_blob_service.get_blob_client(
            container=V_DATASET_CONTAINER,
            blob=blob_path
        )

        # 🔥 overwrite optional (safe fallback)
        data = file_buffer if isinstance(file_buffer, bytes) else file_buffer.getvalue()
        blob.upload_blob(data, overwrite=True)

        return {
            "dataset_name": dataset_name,
            "dataset_path": blob_path
        }

    # =========================
    # RUN PIPELINE
    # =========================
    def run(self, prompt, user_id, job_id=None):

        if not job_id:
            job_id = str(uuid.uuid4())

        print("\n🚀 PIPELINE STARTED")

        # STEP 1: PARSE
        plan = parse_prompt(prompt)
        sources = plan.get("sources", [])

        if not sources:
            raise Exception("No sources detected")

        # STEP 2: INGESTION
        self.ingestion.ingest_sources(
            sources=sources,
            user_id=user_id,
            job_id=job_id
        )

        # STEP 3: DATA MODEL
        model_output = self.datamodel.execute(
            user_id=user_id,
            job_id=job_id
        )

        # 🔥 DEFINE FACT TABLE EARLY (IMPORTANT FIX)
        model_data = model_output["model_output"]["data"]

        fact_table = model_data.get("fact_override") or model_data["model"].get("fact_table")

        if not fact_table:
            raise Exception("Fact table not found")

                # STEP 4: DATASET
            # =========================
        # DATASET NAME (🔥 SINGLE SOURCE OF TRUTH)
        # =========================
        dataset_name = f"{fact_table.lower()}_dataset.csv"

        # =========================
        # DATASET
        # =========================
        dataset_output = self.dataset.execute(
            model_output=model_output,
            user_id=user_id,
            job_id=job_id,
            dataset_name=dataset_name   # ✅ PASS HERE
        )

        # STEP 5: ETL
        etl_output = self.etl.execute(
            dataset=dataset_output["dataset"],
            user_id=user_id,
            job_id=job_id,
            model_output=model_output
        )

        file_buffer = etl_output.get("file_buffer")

        if not file_buffer:
            raise Exception("Missing file buffer from ETL")

        # =========================
        # 🔥 SINGLE DATASET UPLOAD
        # =========================
        dataset_info = self.upload_dataset(
            file_buffer=file_buffer,
            user_id=user_id,
            job_id=job_id,
            dataset_context=fact_table
        )

        # =========================
        # 🔥 ONE LAKE UPLOAD
        # =========================
        onelake_path = self.onelake.upload_file(
            file_buffer=file_buffer,
            user_id=user_id,
            job_id=job_id,
            dataset_name=dataset_info["dataset_name"]
        )

        # =========================
        # DIMENSIONS
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
            "preview": etl_output.get("preview"),
            "dataset_name": dataset_info["dataset_name"],
            "dataset_path": dataset_info["dataset_path"],
            "onelake_path": onelake_path
        }

        return {
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
