import json
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from config import BLOB_CONN_STR, COSMOS_URL, COSMOS_KEY

PIPELINE_CONTAINER = "pipelines"


class PipelineManager:

    def __init__(self):
        # Blob
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        self.container = self.blob_service.get_container_client(PIPELINE_CONTAINER)

        try:
            self.container.create_container()
        except:
            pass

        # Cosmos
        self.cosmos_client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
        self.db = self.cosmos_client.get_database_client("AuthDB")
        self.user_container = self.db.get_container_client("Users")

    # 🔥 NEW: build full pipeline object
    def _build_pipeline(self, user_id, name, jobs):
        now = datetime.utcnow().isoformat() + "Z"

        return {
            "pipeline_id": f"pipe_{uuid.uuid4().hex[:24]}",
            "user_id": user_id,
            "name": name,
            "created_at": now,
            "job_ids": [j["job_id"] for j in jobs],
            "jobs": jobs,  # includes job_name
            "status": "SUCCESS",
            "last_run": now,
            "description": "",
            "last_run_started_at": now,
            "last_run_result": {
                "success_count": len(jobs),
                "total_jobs": len(jobs),
                "failed_jobs": [],
                "failed_details": []
            },
            "updated_at": now,
            "schedule": None
        }

    # 🔥 UPDATED
    def create_pipeline(self, user_id, name, jobs):

        pipeline = self._build_pipeline(user_id, name, jobs)

        # ----------------------
        # 1. SAVE TO BLOB (existing)
        # ----------------------
        path = f"{user_id}/{pipeline['pipeline_id']}.json"

        self.container.get_blob_client(path).upload_blob(
            json.dumps(pipeline, indent=2),
            overwrite=True
        )

        # ----------------------
        # 2. SAVE TO COSMOS (NEW)
        # ----------------------
        try:
            user_doc = self.user_container.read_item(
                item=user_id,
                partition_key=user_id
            )
        except:
            user_doc = {
                "id": user_id,
                "pipelines": []
            }

        if "pipelines" not in user_doc:
            user_doc["pipelines"] = []

        user_doc["pipelines"].append(pipeline)

        self.user_container.upsert_item(user_doc)

        return pipeline

    def get_pipeline(self, user_id, pipeline_id):
        blob = self.container.get_blob_client(f"{user_id}/{pipeline_id}.json")
        return json.loads(blob.download_blob().readall())

    def list_pipelines(self, user_id):
        pipelines = []

        blobs = self.container.list_blobs(name_starts_with=f"{user_id}/")

        for blob in blobs:
            data = json.loads(
                self.container.get_blob_client(blob.name)
                .download_blob()
                .readall()
            )
            pipelines.append(data)

        return pipelines

    def update_schedule(self, user_id, pipeline_id, schedule):
        pipeline = self.get_pipeline(user_id, pipeline_id)
        pipeline["schedule"] = schedule

        # Blob update
        path = f"{user_id}/{pipeline_id}.json"
        self.container.get_blob_client(path).upload_blob(
            json.dumps(pipeline, indent=2),
            overwrite=True
        )

        # 🔥 ALSO UPDATE COSMOS
        user_doc = self.user_container.read_item(
            item=user_id,
            partition_key=user_id
        )

        for p in user_doc["pipelines"]:
            if p["pipeline_id"] == pipeline_id:
                p["schedule"] = schedule
                p["updated_at"] = datetime.utcnow().isoformat() + "Z"

        self.user_container.upsert_item(user_doc)

        return pipeline
