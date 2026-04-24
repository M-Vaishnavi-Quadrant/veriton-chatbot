import json
from datetime import datetime


class PipelineManager:

    def __init__(self, blob_service, scheduler, pipeline_service):
        self.blob = blob_service
        self.pipeline_service = pipeline_service
        self.container = "pipelines"

    def create_pipeline(self, pipeline_name, prompt, user_id, schedule=None):

        container = self.blob.get_container_client(self.container)

        try:
            container.create_container()
        except:
            pass

        path = f"{user_id}/{pipeline_name}.json"

        print(f"[PIPELINE SAVE] {path}")

        data = {
            "pipeline_name": pipeline_name,
            "prompt": prompt,
            "user_id": user_id,
            "created_at": str(datetime.utcnow())
        }

        blob = container.get_blob_client(path)
        blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

        print("[PIPELINE SAVE] SUCCESS")

        return {"pipeline_name": pipeline_name}

    def load_pipeline(self, pipeline_name, user_id):

        container = self.blob.get_container_client(self.container)
        path = f"{user_id}/{pipeline_name}.json"

        blob = container.get_blob_client(path)

        if not blob.exists():
            raise Exception(f"❌ Pipeline NOT FOUND at: {path}")

        data = blob.download_blob().readall()

        return json.loads(data)

    def run_saved_pipeline(self, pipeline_name, user_id):

        pipeline = self.load_pipeline(pipeline_name, user_id)

        return self.pipeline_service.run(
            prompt=pipeline["prompt"],
            user_id=user_id
        )