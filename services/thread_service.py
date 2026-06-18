import json
import uuid

from datetime import datetime

from azure.storage.blob import BlobServiceClient

from config import BLOB_CONN_STR

THREAD_CONTAINER = "threads"


class ThreadService:

    def __init__(self):

        self.blob_service = (
            BlobServiceClient
            .from_connection_string(
                BLOB_CONN_STR
            )
        )

        self.container = (
            self.blob_service
            .get_container_client(
                THREAD_CONTAINER
            )
        )

        try:
            self.container.create_container()
        except:
            pass

    # =====================================================
    # PATH
    # =====================================================

    def _path(
        self,
        thread_id
    ):

        return f"{thread_id}.json"

    # =====================================================
    # CREATE THREAD
    # =====================================================

    def create_thread(

        self,

        user_id,

        job_id,

        title="New Chat"
    ):

        now = datetime.utcnow().isoformat()

        thread = {

            "thread_id": (
                f"thread_{uuid.uuid4().hex}"
            ),

            "user_id": user_id,

            "job_id": job_id,

            "title": title,

            "created_at": now,

            "updated_at": now,

            "messages": [],

            "actions": [],

            "context": {

                "uploaded_datasets": [],

                "generated_datasets": [],

                "selected_dataset": None,

                "etl_completed": False,

                "dq_completed": False,

                "dashboard_completed": False,

                "automl_completed": False,

                "ner_completed": False,

                "latest_dataset_path": None,

                "latest_model_id": None,

                "latest_dashboard_id": None,

                "job_saved": False,

                "conversation_state": None
            }
        }

        self.save_thread(thread)

        # ==========================================
        # INITIAL ASSISTANT MESSAGE
        # ==========================================

        self.add_message(

            thread["thread_id"],

            "assistant",

            "Hello! How can I help you today?"
        )

        return self.load_thread(
            thread["thread_id"]
        )

    # =====================================================
    # SAVE THREAD
    # =====================================================

    def save_thread(
        self,
        thread
    ):

        thread["updated_at"] = (
            datetime.utcnow().isoformat()
        )

        blob = self.container.get_blob_client(

            self._path(
                thread["thread_id"]
            )
        )

        blob.upload_blob(

            json.dumps(
                thread,
                indent=2
            ),

            overwrite=True
        )

    # =====================================================
    # LOAD THREAD
    # =====================================================

    def load_thread(
        self,
        thread_id
    ):

        blob = self.container.get_blob_client(

            self._path(thread_id)
        )

        if not blob.exists():

            return None

        return json.loads(

            blob.download_blob().readall()
        )

    # =====================================================
    # LIST THREADS
    # =====================================================

    def list_threads(
        self,
        user_id,
        job_id
    ):

        threads = []

        blobs = self.container.list_blobs()

        for blob in blobs:

            try:

                raw = (
                    self.container
                    .get_blob_client(blob.name)
                    .download_blob()
                    .readall()
                )

                data = json.loads(raw)

                if (
                    data["user_id"] == user_id
                    and
                    data["job_id"] == job_id
                ):

                    threads.append({

                        "thread_id": (
                            data["thread_id"]
                        ),

                        "title": (
                            data["title"]
                        ),

                        "updated_at": (
                            data["updated_at"]
                        ),

                        "created_at": (
                            data["created_at"]
                        )
                    })

            except:
                continue

        threads.sort(

            key=lambda x: x["updated_at"],

            reverse=True
        )

        return threads

    # =====================================================
    # ADD MESSAGE
    # =====================================================

    def add_message(

        self,

        thread_id,

        role,

        content,

        message_type="text",

        metadata=None
    ):

        thread = self.load_thread(
            thread_id
        )

        if not thread:

            raise Exception(
                "Thread not found"
            )

        thread["messages"].append({

            "message_id": (
                str(uuid.uuid4())
            ),

            "role": role,

            "message_type": message_type,

            "content": content,

            "metadata": metadata or {},

            "timestamp": (
                datetime.utcnow()
                .isoformat()
            )
        })

        self.save_thread(thread)

        return thread

    # =====================================================
    # ADD ACTION
    # =====================================================

    def add_action(
        self,
        thread_id,
        role,
        action_type,
        status,
        request=None,
        response=None
    ):

        thread = self.load_thread(thread_id)

        if not thread:
            raise Exception("Thread not found")

        action = {

            "role":role,

            "action_id": str(uuid.uuid4()),

            "action_type": action_type,

            "status": status,

            "request": request or {},

            "response": response or {},

            "timestamp": datetime.utcnow().isoformat()
        }

        thread["actions"].append(action)

        self.save_thread(thread)

        return action

    # =====================================================
    # UPDATE CONTEXT
    # =====================================================

    def update_context(

        self,

        thread_id,

        updates
    ):

        thread = self.load_thread(
            thread_id
        )

        if not thread:

            raise Exception(
                "Thread not found"
            )

        thread["context"].update(
            updates
        )

        self.save_thread(thread)

        return thread["context"]
    
    def update_thread_name(
        self,
        thread_id,
        thread_name
    ):

        thread = self.load_thread(
            thread_id
        )

        if not thread:
            return

        thread["title"] = thread_name

        self.save_thread(thread)

    # =====================================================
    # ATTACH DATASET
    # =====================================================

    def attach_dataset(

        self,

        thread_id,

        dataset
    ):

        thread = self.load_thread(
            thread_id
        )

        if not thread:

            raise Exception(
                "Thread not found"
            )

        thread["context"][
            "uploaded_datasets"
        ].append(dataset)

        thread["context"][
            "selected_dataset"
        ] = dataset

        self.save_thread(thread)

        return dataset
    
    # =====================================================
    # UPDATE JOB REFERENCES
    # =====================================================

    def update_job_references(

        self,

        thread_id,

        job_id,

        new_job_name=None,

        new_dataset_name=None,

        new_dataset_path=None
    ):

        thread = self.load_thread(thread_id)

        if not thread:
            return

        updated = False

        # ==========================================
        # UPDATE ACTIONS
        # ==========================================

        for action in thread.get("actions", []):

            response = action.get("response", {})

            if response.get("job_id") != job_id:
                continue

            # --------------------------
            # JOB NAME
            # --------------------------

            if new_job_name:

                if "job_name" in response:

                    response["job_name"] = (
                        new_job_name
                    )

            # --------------------------
            # DATASET NAME
            # --------------------------

            if new_dataset_name:

                if "dataset_name" in response:

                    response["dataset_name"] = (
                        new_dataset_name
                    )

                if (
                    "final_dataset" in response
                    and
                    isinstance(
                        response["final_dataset"],
                        dict
                    )
                ):

                    response[
                        "final_dataset"
                    ][
                        "dataset_name"
                    ] = new_dataset_name

            # --------------------------
            # DATASET PATH
            # --------------------------

            if new_dataset_path:

                if "blob_path" in response:

                    response["blob_path"] = (
                        new_dataset_path
                    )

                if "dataset_path" in response:

                    response["dataset_path"] = (
                        new_dataset_path
                    )

                if (
                    "final_dataset" in response
                    and
                    isinstance(
                        response["final_dataset"],
                        dict
                    )
                ):

                    response[
                        "final_dataset"
                    ][
                        "dataset_path"
                    ] = new_dataset_path

                    response[
                        "final_dataset"
                    ][
                        "download_path"
                    ] = new_dataset_path

            updated = True

        if updated:

            self.save_thread(thread)
