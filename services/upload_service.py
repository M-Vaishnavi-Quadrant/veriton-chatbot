import requests
import pandas as pd

from io import BytesIO

from datetime import datetime

from azure.storage.blob import (
    BlobServiceClient
)

from config import (

    V_CONNECTION_STRING,

    BLOB_CONN_STR,

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

    def detect_header_row(
            self,
            df
        ):

            best_row = 0
            best_score = -999

            for idx in range(min(10, len(df))):

                row = df.iloc[idx]

                values = [

                    str(x).strip()

                    for x in row

                    if pd.notna(x)
                ]

                score = 0

                score += len(values)

                score += len(set(values))

                score -= sum(

                    1

                    for v in values

                    if "unnamed" in v.lower()
                )

                if score > best_score:

                    best_score = score

                    best_row = idx

            return best_row

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

        contents,

        sheet_name=None
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

            excel_file = pd.ExcelFile(
                BytesIO(contents)
            )

            sheets = excel_file.sheet_names

            selected_sheet = (

                sheet_name

                if sheet_name

                else sheets[0]
            )

            print(
                f"📌 Selected Sheet: "
                f"{selected_sheet}"
            )

            # ===============================
            # DETECT HEADER ROW
            # ===============================

            raw_df = pd.read_excel(

                BytesIO(contents),

                sheet_name=selected_sheet,

                header=None
            )

            header_row = self.detect_header_row(
                raw_df
            )

            print(
                f"📌 Header Row: "
                f"{header_row}"
            )

            return pd.read_excel(

                BytesIO(contents),

                sheet_name=selected_sheet,

                header=header_row
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

        file,

        sheet_name=None
    ):
        
        try:

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

            # =====================================
            # EXCEL SHEET DETECTION
            # =====================================

            # =====================================
            # EXCEL SHEET DETECTION
            # =====================================

            if file.filename.lower().endswith(
                (".xlsx", ".xls")
            ):

                excel_file = pd.ExcelFile(
                    BytesIO(contents)
                )

                sheets = excel_file.sheet_names

                print(
                    f"📄 Available Sheets: {sheets}"
                )

                # =================================
                # MULTIPLE SHEETS FOUND
                # =================================

                if (

                    len(sheets) > 1

                    and

                    not sheet_name
                ):

                    sheet_response = {

                        "status":
                            "sheet_selection_required",

                        "job_id":
                            job_id,

                        "thread_id":
                            thread_id,

                        "file_name":
                            file.filename,

                        "sheets":
                            sheets
                    }

                    self.thread_service.add_action(

                        thread_id=thread_id,

                        role="assistant",

                        action_type="dataset_upload",

                        status="waiting",

                        request={

                            "filename":
                                file.filename
                        },

                        response=sheet_response
                    )

                    return sheet_response

                # =================================
                # INVALID SHEET
                # =================================

                if (

                    sheet_name

                    and

                    sheet_name not in sheets
                ):

                    sheet_response = {

                        "status":
                            "sheet_selection_required",

                        "job_id":
                            job_id,

                        "thread_id":
                            thread_id,

                        "message":
                            "Please select a valid sheet.",

                        "sheets":
                            sheets,

                        "selected_sheet":
                            sheet_name
                    }

                    self.thread_service.add_action(

                        thread_id=thread_id,

                        role="assistant",

                        action_type="dataset_upload",

                        status="waiting",

                        request={

                            "filename":
                                file.filename,

                            "selected_sheet":
                                sheet_name
                        },

                        response=sheet_response
                    )

                    return sheet_response

                df = self.read_dataset(

                    file.filename,

                    contents,

                    sheet_name
                )

            else:

                df = self.read_dataset(

                    file.filename,

                    contents
                )
            
            selected_sheet = sheet_name

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
                "✅ Uploaded to v dataset container"
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
            # AUTOML REGISTRATION
            # ==========================================

            print(
                f"🚀 Registering dataset with AutoML"
            )

            print(
                f"📂 OneLake Path: "
                f"{onelake_path}"
            )

            automl_response = self.upload_to_automl(

                file_path=onelake_path,

                session_id=session_id,

                user_email=user_email
            )

            automl_registered = (
                automl_response is not None
            )

            print(
                f"✅ AutoML Registered: "
                f"{automl_registered}"
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

                "automl_registered":
                    automl_registered,

                "automl_response":
                    automl_response,

                # ======================================
                # METADATA
                # ======================================

                "metadata": {

                    "rows": len(df),

                    "columns": len(df.columns),

                    "column_names": (
                        list(df.columns)
                    ),

                    "sheet_name":
                        selected_sheet
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
            # ASSISTANT RESPONSE
            # ==========================================

            assistant_response = (

                "✅ Dataset uploaded successfully\n\n"

                f"Job Name: {job_name}\n\n"

                "Dataset Details:\n"

                f"- Rows: {len(df)}\n"

                f"- Columns: {len(df.columns)}\n\n"

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

            upload_response = {

                "success": True,

                "job": {

                    "job_id": job_id,

                    "job_name": job_name,

                    "thread_id": thread_id,

                    "session_id": session_id,

                    "status": "completed",

                    "created_at": doc["created_at"]
                },

                "dataset": {

                    "dataset_name":
                        dataset_display_name,

                    "blob_path":
                        dataset_path,

                    "onelake_path":
                        onelake_path,

                    "source":
                        "upload",

                    "rows":
                        len(df),

                    "columns":
                        len(df.columns),

                    "column_names":
                        list(df.columns),

                    "sheet_name":
                        selected_sheet
                },

                "automl": {

                    "registered":
                        automl_registered,

                    "response":
                        automl_response
                },

                "next_actions": [

                    {
                        "id": "dq",
                        "label": "Apply Data Quality Rules"
                    },

                    {
                        "id": "business_logic",
                        "label": "Apply Business Logic"
                    },

                    {
                        "id": "ner",
                        "label": "Apply Name Entity Resolution"
                    },

                    {
                        "id": "dashboard",
                        "label": "Generate PowerBI Dashboard"
                    },

                    {
                        "id": "automl",
                        "label": "Build AutoML Model"
                    }
                ]
            }

            self.thread_service.add_action(

                thread_id=thread_id,

                role="assistant",

                action_type="dataset_upload",

                status="completed",

                request={

                    "filename":
                        file.filename,

                    "sheet_name":
                        selected_sheet,

                    "user_id":
                        user_id,

                    "job_id":
                        job_id
                },

                response=upload_response
            )


            # ==========================================
            # RESPONSE
            # ==========================================

            return upload_response
        
        except Exception as e:

                error_response = {

                    "status": "failed",

                    "error": str(e),

                    "job_id": job_id,

                    "thread_id": thread_id,

                    "filename": file.filename
                }

                try:

                    self.thread_service.add_message(

                        thread_id=thread_id,

                        role="assistant",

                        content=str(e),

                        message_type="error"
                    )

                    self.thread_service.add_action(

                        thread_id=thread_id,

                        role="assistant",

                        action_type="dataset_upload",

                        status="failed",

                        request={

                            "filename":
                                file.filename,

                             "sheet_name":
                                sheet_name,

                            "job_id":
                                job_id,
                            
                        },

                        response=error_response
                    )

                except Exception:
                    pass

                raise
