import requests
import pandas as pd

from io import BytesIO

from datetime import datetime

from azure.storage.blob import (
    BlobServiceClient
)

from config import (

    V_CONNECTION_STRING,

    USER_CONTAINER,

    V_DATASET_CONTAINER
)

from services.onelake_service import (
    OneLakeService
)

from services.cosmos_service import (
    CosmosService
)

from services.thread_service import (
    ThreadService
)


class UploadService:

    def __init__(self):

        self.blob_service = (
            BlobServiceClient
            .from_connection_string(
                V_CONNECTION_STRING
            )
        )

        self.onelake_service = (
            OneLakeService()
        )

        self.cosmos_service = (
            CosmosService()
        )

        self.thread_service = (
            ThreadService()
        )

    # =====================================================
    # AUTOML REGISTRATION
    # =====================================================

    def upload_to_automl(

        self,

        file_path,

        session_id,

        user_email
    ):

        try:

            payload = {

                "file_path": file_path,

                "session_id": session_id,

                "user_email": user_email,

                "query": ""
            }

            headers = {

                "accept": "application/json",

                "Content-Type":
                    "application/x-www-form-urlencoded"
            }

            response = requests.post(

                "https://api.veriton.ai/api/service3/upload_file_V",

                data=payload,

                headers=headers,

                timeout=300
            )

            print(
                f"✅ AutoML Status: "
                f"{response.status_code}"
            )

            print(
                f"✅ AutoML Response: "
                f"{response.text}"
            )

            if response.status_code != 200:

                return None

            return response.json()

        except Exception as e:

            print(
                f"❌ AutoML Upload Error: "
                f"{str(e)}"
            )

            return None

    # =====================================================
    # READ DATASET
    # =====================================================

    def read_dataset(

        self,

        file_name,

        contents
    ):

        file_name = file_name.lower()

        # CSV
        if file_name.endswith(".csv"):

            return pd.read_csv(
                BytesIO(contents)
            )

        # EXCEL
        elif (

            file_name.endswith(".xlsx")

            or

            file_name.endswith(".xls")
        ):

            return pd.read_excel(
                BytesIO(contents)
            )

        # PARQUET
        elif file_name.endswith(".parquet"):

            return pd.read_parquet(
                BytesIO(contents)
            )

        # JSON
        elif file_name.endswith(".json"):

            return pd.read_json(
                BytesIO(contents)
            )

        else:

            raise Exception(

                f"Unsupported file type: "
                f"{file_name}"
            )

    # =====================================================
    # CONVERT TO CSV
    # =====================================================

    def convert_to_csv(
        self,
        df
    ):

        csv_buffer = BytesIO()

        df.to_csv(

            csv_buffer,

            index=False
        )

        csv_buffer.seek(0)

        return csv_buffer.getvalue()

    # =====================================================
    # MAIN UPLOAD FLOW
    # =====================================================

    async def upload_dataset(

        self,

        user_id,

        job_id,

        thread_id,

        session_id,

        user_email,

        file
    ):

        # ==========================================
        # VALIDATE THREAD
        # ==========================================

        thread = self.thread_service.load_thread(
            thread_id
        )

        if not thread:

            raise Exception(
                "Invalid thread_id"
            )

        # ==========================================
        # READ FILE
        # ==========================================

        contents = await file.read()

        df = self.read_dataset(

            file.filename,

            contents
        )

        # ==========================================
        # CONVERT TO CSV
        # ==========================================

        contents = self.convert_to_csv(
            df
        )

        # ==========================================
        # DATASET + JOB NAME
        # ==========================================

        base_name = (
            file.filename
            .split(".")[0]
        )

        job_name = (

            base_name
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
            .strip()
        )

        # ==========================================
        # DATASET NAME
        # ==========================================

        dataset_display_name = (
            f"{job_name}_dataset"
        )

        dataset_file_name = (
            f"{job_name}_dataset.csv"
        )

        # ==========================================
        # RAW PATH
        # ==========================================

        dataset_path = (

            f"{user_id}/"
            f"{job_id}/"
            f"{dataset_file_name}"
        )


        # ==========================================
        # FINAL DATASET CONTAINER
        # ==========================================

        dataset_blob = (

            self.blob_service
            .get_blob_client(

                container=V_DATASET_CONTAINER,

                blob=dataset_path
            )
        )

        dataset_blob.upload_blob(

            contents,

            overwrite=True
        )

        print(
            "✅ Uploaded to V_DATASET_CONTAINER"
        )

        # ==========================================
        # ONELAKE
        # ==========================================

        onelake_path = (

            self.onelake_service
            .upload_file(

                BytesIO(contents),

                user_id,

                job_id,

                dataset_file_name
            )
        )

        print(
            f"✅ Uploaded to OneLake"
        )

        # ==========================================
        # DATASET INFO
        # ==========================================

        dataset_info = {

            "job_name": job_name,

            "dataset_name": dataset_display_name,

            "blob_path": dataset_path,

            "onelake_path": onelake_path,

            "source": "upload",

            "rows": len(df),

            "columns": len(df.columns),

            "column_names": (
                list(df.columns)
            )
        }

        # ==========================================
        # COSMOS DOC
        # ==========================================

        doc = {

            "id": job_id,

            "job_id": job_id,

            "job_name": job_name,

            "thread_id": thread_id,

            "session_id": session_id,

            "user_email": user_email,

            "user_id": user_id,

            "created_at": (

                datetime.utcnow()
                .isoformat()

                + "Z"
            ),

            "completed_at": (

                datetime.utcnow()
                .isoformat()

                + "Z"
            ),

            "status": "completed",

            # ======================================
            # SCHEDULE
            # ======================================

            "schedule": {

                "frequency": None,

                "time_utc": None,

                "scheduled_at": None,

                "active": True
            },

            # ======================================
            # DATASET
            # ======================================

            "dataset_name": dataset_display_name,

            "dataset_type": "uploaded",

            "source_type": "upload",

            "dataset_path": dataset_path,

            "onelake_path": onelake_path,

            "current_stage": "uploaded",

            "saved": True,
    

            # ======================================
            # PIPELINE FLAGS
            # ======================================

            "dq": False,

            "ner": False,

            "business_logic": False,

            "dashboard": False,

            "automl": False,

            # ======================================
            # METADATA
            # ======================================

            "metadata": {

                "rows": len(df),

                "columns": len(df.columns),

                "column_names": (
                    list(df.columns)
                )
            },

            # ======================================
            # SOURCES
            # ======================================

            "sources": [

                "chatbot"
            ],

            # ======================================
            # RESULTS
            # ======================================

            "results": [

                {
                    "source": "chatbot",

                    "status": "completed",

                    "effective_destination": (
                        onelake_path
                    )
                }
            ]
        }

        # ==========================================
        # SAVE TO COSMOS
        # ==========================================

        self.cosmos_service.save_dataset(

            user_id,

            doc
        )
        # ==========================================
        # CREATED DATASET
        # ==========================================

        dataset_doc = {

            "job_id": job_id,

            "custom_table_name": (

                dataset_display_name
            ),

            "request_body": {

                "user_id": user_id,

                "job_id": job_id,

                "custom_table_name": (

                    dataset_display_name
                ),

                "column_mappings": [],

                "join_type": "INNER"
            },

            "file_path": dataset_path,

            "rows": len(df),

            "columns": list(df.columns),

            "timestamp": (

                datetime.utcnow()
                .isoformat()
            )
        }

        self.cosmos_service.save_create_dataset(

            user_id,

            dataset_doc
        )

        print(
            "✅ Saved to Cosmos"
        )

        # ==========================================
        # ATTACH DATASET TO THREAD
        # ==========================================

        self.thread_service.attach_dataset(

            thread_id,

            dataset_info
        )

        # ==========================================
        # SAVE USER MESSAGE
        # ==========================================

        self.thread_service.add_message(

            thread_id=thread_id,

            role="user",

            content=(
                f"Uploaded dataset "
                f"{dataset_display_name}"
            ),

            message_type="dataset"
        )

        # ==========================================
        # SAVE ACTION
        # ==========================================

        self.thread_service.add_action(

            thread_id,

            {

                "type": "file_upload",

                "status": "completed",

                "job_name": job_name,

                "dataset_name": dataset_display_name,

                "dataset_path": dataset_path,

                "onelake_path": onelake_path,

                "job_id": job_id,

                "rows": len(df),

                "columns": len(df.columns)
            }
        )

        # ==========================================
        # ASSISTANT RESPONSE
        # ==========================================

        assistant_response = (

            "✅ Dataset uploaded successfully\n\n"

            f"Job Name: {job_name}\n\n"

            "Dataset Details:\n"

            f"- Rows: {len(df)}\n"

            f"- Columns: {len(df.columns)}\n\n"

            "Suggested Next Actions:\n"

            "- Run ETL\n"

            "- Apply DQ Rules\n"

            "- Apply Business Logic\n"

            "- Run NER\n"

            "- Generate Dashboard\n"

            "- Build AutoML Model"
        )

        # ==========================================
        # SAVE ASSISTANT MESSAGE
        # ==========================================

        self.thread_service.add_message(

            thread_id=thread_id,

            role="assistant",

            content=assistant_response,

            message_type="completion",

            metadata={

                "dataset": dataset_info
            }
        )

        # ==========================================
        # UPDATE THREAD CONTEXT
        # ==========================================

        self.thread_service.update_context(

            thread_id,

            {

                "job_name": job_name,

                "selected_dataset": (
                    dataset_info
                ),

                "latest_dataset_path": (
                    dataset_path
                ),

            }
        )

        # ==========================================
        # RESPONSE
        # ==========================================

        return {

            "success": True,

            "job_id": job_id,

            "job_name": job_name,

            "thread_id": thread_id,

            "session_id": session_id,

            "response": assistant_response,

            "data": doc,

            "dataset": dataset_info,

            "next_actions": [

                "etl",

                "dq",

                "business_logic",

                "ner",

                "dashboard",

                "automl"
            ]
        }