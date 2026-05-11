import json
import uuid
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR

PIPELINE_CONTAINER = "pipelines"


class PipelineManager:

    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        self.container = self.blob_service.get_container_client(PIPELINE_CONTAINER)

        try:
            self.container.create_container()
        except:
            pass

    def create_pipeline(self, user_id, name, jobs):
        pipeline_id = str(uuid.uuid4())

        pipeline = {
            "pipeline_id": pipeline_id,
            "user_id": user_id,
            "name": name,
            "jobs": jobs,
            "schedule": None
        }

        path = f"{user_id}/{pipeline_id}.json"

        self.container.get_blob_client(path).upload_blob(
            json.dumps(pipeline, indent=2),
            overwrite=True
        )

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

        path = f"{user_id}/{pipeline_id}.json"

        self.container.get_blob_client(path).upload_blob(
            json.dumps(pipeline, indent=2),
            overwrite=True
        )

        return pipeline
