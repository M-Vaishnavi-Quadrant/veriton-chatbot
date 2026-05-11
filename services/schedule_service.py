# services/schedule_service.py

import json
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR

SCHEDULE_CONTAINER = "pipeline-schedules"


class ScheduleService:

    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        self.container = self.blob_service.get_container_client(SCHEDULE_CONTAINER)

        try:
            self.container.create_container()
        except:
            pass

    def save_schedule(self, schedule):

        path = f"{schedule['user_id']}/{schedule['pipeline_id']}.json"

        self.container.get_blob_client(path).upload_blob(
            json.dumps(schedule, indent=2),
            overwrite=True
        )

    def get_all_schedules(self):

        schedules = []

        for blob in self.container.list_blobs():
            try:
                raw = self.container.get_blob_client(blob.name).download_blob().readall()

                if not raw or raw.strip() == b"":
                    print(f"⚠️ Skipping empty file: {blob.name}")
                    continue

                data = json.loads(raw)

                schedules.append(data)

            except Exception as e:
                print(f"⚠️ Skipping bad schedule: {blob.name} | {str(e)}")
                continue

        return schedules