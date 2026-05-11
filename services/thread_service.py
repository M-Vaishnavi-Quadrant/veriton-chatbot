import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR

THREAD_CONTAINER = "threads"


class ThreadService:

    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        self.container = self.blob_service.get_container_client(THREAD_CONTAINER)

        try:
            self.container.create_container()
        except:
            pass

    # =========================
    # PATH
    # =========================
    def _path(self, user_id, job_id):
        return f"{user_id}/{job_id}.json"

    # =========================
    # CREATE THREAD
    # =========================
    def create_thread(self, user_id, job_id):
        thread = {
            "thread_id": f"{user_id}_{job_id}",
            "user_id": user_id,
            "job_id": job_id,
            "created_at": datetime.utcnow().isoformat(),
            "messages": [],
            "actions": [],
            "job_summary": {
                "etl_status": "pending",
                "automl_status": "pending",
                "powerbi_status": "pending",
                "dataset": None
            }
        }

        self.save_thread(user_id, job_id, thread)
        return thread

    # =========================
    # LOAD THREAD
    # =========================
    def load_thread(self, user_id, job_id):
        blob = self.container.get_blob_client(self._path(user_id, job_id))

        if not blob.exists():
            return self.create_thread(user_id, job_id)

        try:
            thread = json.loads(blob.download_blob().readall())
        except:
            return self.create_thread(user_id, job_id)

        # Remove old fields
        thread.pop("latest", None)

        return thread

    # =========================
    # SAVE THREAD
    # =========================
    def save_thread(self, user_id, job_id, thread):
        self.container.get_blob_client(self._path(user_id, job_id)).upload_blob(
            json.dumps(thread, indent=2),
            overwrite=True
        )

    # =========================
    # ADD ACTION (ALLOW MULTIPLE RUNS)
    # =========================
    def add_action(self, user_id, job_id, payload):

        thread = self.load_thread(user_id, job_id)

        action_type = payload.get("type")

        # =========================
        # ADD TIMESTAMP
        # =========================
        payload["timestamp"] = datetime.utcnow().isoformat()

        # =========================
        # 🔥 ENSURE job_name ALWAYS EXISTS
        # =========================
        if action_type == "etl":

            if not payload.get("job_name"):
                try:
                    fact = payload.get("response", {}).get("data_model", {}).get("fact_table")
                    if fact:
                        payload["job_name"] = f"{fact}_job"
                    else:
                        payload["job_name"] = f"job_{job_id[:6]}"
                except:
                    payload["job_name"] = f"job_{job_id[:6]}"

        # =========================
        # ADD ACTION (NO DUPLICATE BLOCK)
        # =========================
        thread["actions"].append(payload)

        # =========================
        # UPDATE STATUS
        # =========================
        if action_type == "etl":
            thread["job_summary"]["etl_status"] = "completed"

            try:
                dataset_name = payload["response"]["final_dataset"]["dataset_name"]
                thread["job_summary"]["dataset"] = dataset_name
            except:
                pass

        elif action_type == "automl":
            thread["job_summary"]["automl_status"] = "completed"

        elif action_type == "powerbi":
            thread["job_summary"]["powerbi_status"] = "completed"

        elif action_type == "file_upload":
            thread["job_summary"]["file_uploaded"] = True

        self.save_thread(user_id, job_id, thread)

        return thread

    # =========================
    # GET THREAD
    # =========================
    def get_thread(self, user_id, job_id):

        thread = self.load_thread(user_id, job_id)

        for action in thread.get("actions", []):

            if action.get("type") == "etl":

                # ✅ Ensure job_name exists (backward compatibility)
                if not action.get("job_name"):
                    jid = action.get("job_id")
                    try:
                        fact = action.get("response", {}).get("data_model", {}).get("fact_table")
                        if fact:
                            action["job_name"] = f"{fact}_job"
                        elif jid:
                            action["job_name"] = f"job_{jid[:6]}"
                    except:
                        if jid:
                            action["job_name"] = f"job_{jid[:6]}"

                # ✅ Ensure download_url exists
                if not action.get("download_url"):
                    try:
                        dataset_name = action["response"]["final_dataset"]["dataset_name"]
                        action["download_url"] = f"/download/{user_id}/{job_id}/{dataset_name}"
                    except:
                        action["download_url"] = None

        return thread

    # =========================
    # GET ALL THREADS
    # =========================
    def get_threads_by_user(self, user_id):

        threads = []

        blobs = self.container.list_blobs(name_starts_with=f"{user_id}/")

        for blob in blobs:
            try:
                data = json.loads(
                    self.container.get_blob_client(blob.name)
                    .download_blob()
                    .readall()
                )

                data.pop("latest", None)

                for action in data.get("actions", []):

                    if action.get("type") == "etl":

                        # job_name fix
                        if not action.get("job_name"):
                            jid = action.get("job_id")
                            action["job_name"] = f"job_{jid[:6]}" if jid else "unknown_job"

                        # download_url fix
                        try:
                            dataset_name = action["response"]["final_dataset"]["dataset_name"]
                            action["download_url"] = f"/download/{user_id}/{data['job_id']}/{dataset_name}"
                        except:
                            action["download_url"] = None

                threads.append(data)

            except:
                continue

        return threads
