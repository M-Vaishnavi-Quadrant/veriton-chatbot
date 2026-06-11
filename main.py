# =========================
# IMPORTS
# =========================

import json
import re
import time
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from openai import AzureOpenAI
from pydantic import BaseModel
from fastapi.responses import StreamingResponse

from config import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_OPENAI_ENDPOINT,
    BLOB_CONN_STR,
    USER_CONTAINER,
    V_CONNECTION_STRING,
    V_DATASET_CONTAINER,
)
from services import powerbi_service
from services.business_logic.business_logic_service import (
    BusinessLogicService
)
from services.chat_orchestrator import ChatOrchestrator
from services.cosmos_service import CosmosService
from services.dq_service import DQService
from services.DQ_rules.dq_generator import DQRuleGenerator
from services.ner_service import NERService
from services.onelake_service import OneLakeService
from services.pipeline_manager import PipelineManager
from services.pipeline_service import PipelineService
from services.powerbi_service import generate_powerbi_dashboard, sanitize_for_json
from services.schedule_service import ScheduleService
from services.scheduler_service import schedule_pipeline
from services.thread_service import ThreadService
from services.upload_service import UploadService


# =========================
# CLIENTS & SERVICES
# =========================

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION,
)

app = FastAPI(title="Job Execution System")

blob_service = BlobServiceClient.from_connection_string(V_CONNECTION_STRING)
cosmos_service = CosmosService()
pipeline_service = PipelineService()
dq_service = DQService(client)
dq_generator = DQRuleGenerator(client)
business_service = BusinessLogicService(client)
ner_service = NERService(client)
thread_service = ThreadService()
pipeline_manager = PipelineManager()
schedule_service = ScheduleService()
onelake_service = OneLakeService()
upload_service = UploadService()
orchestrator = ChatOrchestrator(client)
business_logic_service = BusinessLogicService(client)


# =========================
# REQUEST MODELS
# =========================

class RunRequest(BaseModel):
    user_id: str
    job_id: str
    thread_id: str
    prompt: str


class SaveJobRequest(BaseModel):

    user_id: str

    job_id: str

    thread_id: str

    session_id: str

    user_email: str

    frequency: str | None = None

    time_utc: str | None = None


class RenameJobRequest(BaseModel):
    user_id: str
    job_id: str
    thread_id: str
    new_name: str


class RenameDatasetRequest(BaseModel):
    user_id: str
    job_id: str
    thread_id: str
    new_name: str


class PowerBIDashboardRequest(BaseModel):
    csv_blob: str
    user_prompt: str
    user_id: str
    job_id: str
    thread_id: str


class AutoMLRequest(BaseModel):
    user_id: str
    job_id: str
    session_id: str
    user_email: str
    query: str
    thread_id: str


class DQRuleRequest(BaseModel):
    blob_path: str
    rules: List[Dict[str, Any]]


class GenerateDQRequest(BaseModel):
    blob_path: str


class CreateThreadRequest(BaseModel):
    user_id: str
    job_id: str
    title: str = "New Chat"


class ChatRequest(BaseModel):
    user_id: str
    job_id: str
    thread_id: str
    user_email: str
    message: str
    selected_jobs: Optional[List[str]] = None


class AttachDatasetRequest(BaseModel):
    thread_id: str
    dataset: dict


# =========================
# HELPERS
# =========================

def generate_job_id() -> str:
    return uuid.uuid4().hex

def generate_pipeline_id():

    return (
        "pipe_"
        + uuid.uuid4().hex[:24]
    )


def extract_pipeline_name(message: str) -> str:
    message = message.lower()
    keywords = ["daily", "weekly", "monthly", "no", "yes"]
    words = message.split()
    filtered = []
    for w in words:
        if w in keywords:
            break
        if re.match(r"\d{1,2}:\d{2}", w):
            break
        filtered.append(w)
    return "_".join(filtered) if filtered else "pipeline"


def parse_pipeline_prompt(message: str):
    message = message.lower().strip()
    message = message.replace(";", ":").replace(" at ", " ")
    pipeline_name = extract_pipeline_name(message)

    if "no" in message:
        return {"pipeline_name": pipeline_name, "schedule": None}

    freq = None
    for f in ["daily", "weekly", "monthly"]:
        if f in message:
            freq = f
            break

    time_match = re.search(r"\d{1,2}:\d{2}", message)
    if not freq or not time_match:
        return None

    hour, minute = map(int, time_match.group().split(":"))
    schedule = {"type": freq, "hour": hour, "minute": minute}

    if freq == "weekly":
        cleaned = re.sub(r"\d{1,2}:\d{2}", "", message)
        day_match = re.search(r"\b[0-6]\b", cleaned)
        if day_match:
            schedule["day"] = day_match.group()

    if freq == "monthly":
        cleaned = re.sub(r"\d{1,2}:\d{2}", "", message)
        day_match = re.search(r"\b(0?[1-9]|[12][0-9]|3[01])\b", cleaned)
        if day_match:
            schedule["day"] = str(int(day_match.group()))

    return {"pipeline_name": pipeline_name, "schedule": schedule}


def safe_json_loads(text: str):
    if not text:
        raise ValueError("LLM returned empty response")
    text = text.strip()
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"^```", "", text)
    text = re.sub(r"```$", "", text)
    match = re.search(r"(\[.*\]|\{.*\})", text, re.DOTALL)
    if match:
        text = match.group(1)
    return json.loads(text)


def validate_generated_rules(rules):
    validated = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if not rule.get("rule"):
            continue
        if not rule.get("description"):
            continue
        if rule.get("severity") not in ["high", "medium", "low"]:
            rule["severity"] = "medium"
        validated.append(rule)
    return validated


def get_user_jobs(user_id: str):
    container = pipeline_service.blob_service.get_container_client("jobs")
    jobs = []
    seen = set()
    for blob in container.list_blobs(name_starts_with=f"{user_id}/"):
        if blob.name.endswith(".json") and "temp_result" not in blob.name:
            data = json.loads(
                container.get_blob_client(blob.name).download_blob().readall()
            )
            job_id = data.get("job_id")
            if job_id in seen:
                continue
            seen.add(job_id)
            jobs.append({
                "job_id": job_id,
                "job_name": data.get("job_name", f"Job {job_id[:6]}"),
            })
    return jobs

def convert_numpy(obj):

    if isinstance(obj, dict):
        return {str(k): convert_numpy(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [convert_numpy(i) for i in obj]

    elif isinstance(obj, tuple):
        return [convert_numpy(i) for i in obj]

    elif isinstance(obj, np.integer):
        return int(obj)

    elif isinstance(obj, np.floating):
        return float(obj)

    elif isinstance(obj, np.bool_):
        return bool(obj)

    elif isinstance(obj, np.ndarray):
        return obj.tolist()

    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")

    elif isinstance(obj, pd.Series):
        return obj.tolist()

    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    return obj


def run_pipeline_execution(user_id: str, pipeline_id: str):
    print(f"\n🚀 Running pipeline: {pipeline_id}")
    pipeline = pipeline_manager.get_pipeline(user_id, pipeline_id)
    for job_id in pipeline["jobs"]:
        try:
            result = pipeline_service.run(
                prompt="pipeline run", user_id=user_id, job_id=job_id
            )
            thread_service.add_action(user_id, job_id, {
                "type": "etl",
                "prompt": "pipeline run",
                "response": result,
            })
        except Exception as e:
            print("❌ Job failed:", job_id, str(e))

def generate_job_name(name: str):

    return (

        name
        .replace(".csv", "")
        .replace(".xlsx", "")
        .replace(".xls", "")
        .replace(".parquet", "")
        .replace(" ", "_")
        .replace("-", "_")
        .lower()
        .strip()
    )


def generate_dataset_name(job_name: str):

    return f"{job_name}_dataset"


def generate_dataset_file(job_name: str):

    return f"{job_name}_dataset.csv"


# =========================
# RUN PIPELINE
# =========================

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from io import BytesIO
import numpy as np
import pandas as pd
import uuid


# ==========================================
# SAFE NUMPY/PANDAS CONVERTER
# ==========================================

def convert_numpy(obj):

    if isinstance(obj, dict):
        return {str(k): convert_numpy(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [convert_numpy(i) for i in obj]

    elif isinstance(obj, tuple):
        return [convert_numpy(i) for i in obj]

    elif isinstance(obj, np.integer):
        return int(obj)

    elif isinstance(obj, np.floating):
        return float(obj)

    elif isinstance(obj, np.bool_):
        return bool(obj)

    elif isinstance(obj, np.ndarray):
        return obj.tolist()

    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")

    elif isinstance(obj, pd.Series):
        return obj.tolist()

    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    return obj


# ==========================================
# RUN API
# ==========================================

@app.post("/run")
def run(req: RunRequest):

    try:

        print("\n🚀 ETL PIPELINE STARTED")

        # ==========================================
        # LOAD THREAD
        # ==========================================

        thread = thread_service.load_thread(
            req.thread_id
        )

        if not thread:

            raise HTTPException(
                status_code=404,
                detail="Thread not found"
            )

        # ==========================================
        # ADD USER MESSAGE
        # ==========================================

        thread_service.add_message(
            thread_id=req.thread_id,
            role="user",
            content=req.prompt,
            message_type="text",
        )

        # ==========================================
        # STATUS MESSAGE
        # ==========================================

        thread_service.add_message(
            thread_id=req.thread_id,
            role="assistant",
            content="Starting ETL pipeline...",
            message_type="status",
        )

        # ==========================================
        # RUN PIPELINE
        # ==========================================

        result = pipeline_service.run(
            prompt=req.prompt,
            user_id=req.user_id,
            job_id=req.job_id,
        )

        print("✅ ETL completed")

        # ==========================================
        # GET FINAL DATASET
        # ==========================================

        final_dataset = result.get("final_dataset") or {}

        dataset_df = final_dataset.get("dataframe")

        if dataset_df is None:
            raise Exception("Pipeline did not return dataframe")


        # ==========================================
        # JOB NAME
        # ==========================================

        original_dataset_name = (
            final_dataset.get("dataset_name")
            or f"{req.job_id}.csv"
        )

        base_name = (
            original_dataset_name
            .replace(".csv", "")
        )

        # Remove _dataset suffix if present
        if base_name.endswith("_dataset"):
            base_name = base_name[:-8]

        job_name = (
            base_name
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
            .strip()
        )

        print(f"📌 Job Name: {job_name}")

        # ==========================================
        # DATASET DISPLAY NAME
        # ==========================================

        dataset_display_name = (
            f"{job_name}_dataset"
        )

        # ==========================================
        # DATASET FILE NAME
        # ==========================================

        dataset_file_name = (
            f"{job_name}_dataset.csv"
        )

        # ==========================================
        # DATASET PATH
        # ==========================================

        dataset_path = (
            f"{req.user_id}/"
            f"{req.job_id}/"
            f"{dataset_file_name}"
        )

        # ==========================================
        # SAVE DATAFRAME TO CSV
        # ==========================================

        csv_buffer = BytesIO()

        dataset_df.to_csv(
            csv_buffer,
            index=False
        )

        csv_buffer.seek(0)

        # ==========================================
        # UPLOAD DATASET
        # ==========================================

        dataset_blob = (
            upload_service
            .blob_service
            .get_blob_client(
                container=V_DATASET_CONTAINER,
                blob=dataset_path
            )
        )

        dataset_blob.upload_blob(
            csv_buffer.getvalue(),
            overwrite=True
        )

        print("✅ Dataset uploaded")

        # ==========================================
        # REMOVE DATAFRAME
        # ==========================================

        if "dataframe" in final_dataset:
            del final_dataset["dataframe"]

        # ==========================================
        # UPDATE FINAL DATASET
        # ==========================================

        final_dataset["dataset_name"] = (
            dataset_display_name
        )

        final_dataset["dataset_path"] = (
            dataset_path
        )

        final_dataset["download_path"] = (
            dataset_path
        )

        # ==========================================
        # DOWNLOAD URL
        # ==========================================

        download_url = (
            f"/download/"
            f"{req.user_id}/"
            f"{req.job_id}/"
            f"{dataset_file_name}"
        )

        # ==========================================
        # DATASET INFO
        # ==========================================

        dataset_info = {

            "dataset_name": (
                dataset_display_name
            ),

            "blob_path": (
                dataset_path
            ),

            "source": "etl",

            "rows": int(
                final_dataset.get("rows", 0)
            ),

            "columns": int(
                len(
                    final_dataset.get(
                        "columns",
                        []
                    )
                )
            ),
        }

        # ==========================================
        # UPDATE THREAD CONTEXT
        # ==========================================

        thread_service.update_context(
            req.thread_id,
            {
                "etl_completed": True,
                "job_name": job_name,
                "selected_dataset": dataset_info,
                "latest_dataset_path": dataset_path,
            },
        )

        # ==========================================
        # ADD COMPLETION MESSAGE
        # ==========================================

        thread_service.add_message(
            thread_id=req.thread_id,
            role="assistant",
            content=(

                f"✅ ETL completed successfully\n\n"

                f"Dataset: {dataset_display_name}\n"

                f"Rows: {final_dataset.get('rows', 0)}\n"

                f"Columns: {len(final_dataset.get('columns', []))}"
            ),
            message_type="completion",
            metadata={
                "download_url": download_url,
                "dataset_name": dataset_display_name,
                "dataset_path": dataset_path,
            },
        )

        # ==========================================
        # RESPONSE
        # ==========================================

        response_data = {

            "status": "success",

            "job_id": req.job_id,

            "thread_id": req.thread_id,

            "job_name": job_name,

            "data_model": result.get("data_model"),

            "relationships": result.get("relationships"),

            "schemas": result.get("schemas"),

            "final_dataset": final_dataset,

            "download_url": download_url,

            "saved": False,

            "next_actions": [
                {
                    "action": "dq",
                    "label": "Apply Data Quality Rules"
                },
                {
                    "action": "business_logic",
                    "label": "Apply Business Logic"
                },
                {
                    "action": "ner",
                    "label": "Apply Name Entity Resolution"
                },
                {
                    "action": "dashboard",
                    "label": "Generate Power BI Dashboard"
                },
                {
                    "action": "automl",
                    "label": "Build AutoML Model"
                },
                {
                    "action": "pipeline",
                    "label": "Create Pipeline"
                }
            ]
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="etl",

            status="completed",

            request={

                "prompt": req.prompt,

                "job_id": req.job_id,

                "user_id": req.user_id
            },

            response=response_data
        )

        # ==========================================
        # SAFE SERIALIZATION
        # ==========================================

        safe_response = convert_numpy(
            response_data
        )

        return jsonable_encoder(
            safe_response
        )

    except Exception as e:

        error_response = {

            "status": "failed",

            "error": str(e),

            "job_id": req.job_id
        }

        thread_service.add_message(

            thread_id=req.thread_id,

            role="assistant",

            content=str(e),

            message_type="error"
        )

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="etl",

            status="failed",

            request={

                "prompt": req.prompt,

                "job_id": req.job_id
            },

            response=error_response
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
# =========================
# DOWNLOAD DATASET
# =========================

@app.get("/download/{user_id}/{job_id}/{file_name}")
def download_dataset(
    user_id: str,
    job_id: str,
    file_name: str
):

    try:

        # ==========================================
        # BUILD BLOB PATH
        # ==========================================

        blob_path = (
            f"{user_id}/"
            f"{job_id}/"
            f"{file_name}"
        )

        print(
            f"📥 Download requested: {blob_path}"
        )

        # ==========================================
        # GET BLOB CLIENT
        # ==========================================

        blob_client = blob_service.get_blob_client(

            container=V_DATASET_CONTAINER,

            blob=blob_path
        )

        # ==========================================
        # VALIDATE FILE EXISTS
        # ==========================================

        if not blob_client.exists():

            raise HTTPException(

                status_code=404,

                detail=(
                    f"Dataset not found: "
                    f"{blob_path}"
                )
            )

        # ==========================================
        # DOWNLOAD STREAM
        # ==========================================

        stream = blob_client.download_blob()

        print(
            f"✅ Downloading: {file_name}"
        )

        # ==========================================
        # RETURN FILE
        # ==========================================

        return StreamingResponse(

            stream.chunks(),

            media_type="application/octet-stream",

            headers={

                "Content-Disposition":
                    f'attachment; filename="{file_name}"',

                "Cache-Control":
                    "no-cache"
            }
        )

    except HTTPException:

        raise

    except Exception as e:

        print(
            "❌ DOWNLOAD ERROR:",
            str(e)
        )

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

# =========================
# SAVE JOB
# =========================

@app.post("/save-job")
def save_job(req: SaveJobRequest):

    try:

        # ==========================================
        # LOAD THREAD
        # ==========================================

        thread = thread_service.load_thread(
            req.thread_id
        )

        if not thread:

            raise HTTPException(
                status_code=404,
                detail="Thread not found"
            )

        # ==========================================
        # THREAD CONTEXT
        # ==========================================

        context = thread["context"]

        job_name = context.get(
                "job_name",
                ""
            )

        # normalize job name
        job_name = (
            job_name
            .replace(".csv", "")
            .strip()
        )

        while job_name.endswith("_dataset"):
            job_name = job_name[:-8]

        dataset = context.get(
            "selected_dataset"
        )

        if not dataset:

            raise HTTPException(
                status_code=400,
                detail="No dataset available to save."
            )

        # ==========================================
        # DATASET NAMES
        # ==========================================

        dataset_display_name = (
            f"{job_name}_dataset"
        )

        dataset_file_name = (
            f"{job_name}_dataset.csv"
        )

        dataset_path = (
            f"{req.user_id}/"
            f"{req.job_id}/"
            f"{dataset_file_name}"
        )

        # ==========================================
        # LOAD DATASET
        # ==========================================

        blob_client = (

            upload_service
            .blob_service
            .get_blob_client(

                container=V_DATASET_CONTAINER,

                blob=dataset_path
            )
        )

        if not blob_client.exists():

            raise Exception(
                f"Dataset not found: "
                f"{dataset_path}"
            )

        dataset_bytes = (

            blob_client
            .download_blob()
            .readall()
        )

        print(
            "✅ Dataset loaded"
        )

        # ==========================================
        # SAVE TO ONELAKE
        # ==========================================

        onelake_path = (

            upload_service
            .onelake_service
            .upload_file(

                BytesIO(dataset_bytes),

                req.user_id,

                req.job_id,

                dataset_file_name
            )
        )

        print(
            "✅ Saved to OneLake"
        )

        # ==========================================
        # REGISTER TO AUTOML
        # ==========================================

        automl_response = (

            upload_service
            .upload_to_automl(

                file_path=onelake_path,

                session_id=req.session_id,

                user_email=req.user_email,
            )
        )

        print(
            "✅ Registered to AutoML"
        )

        # ==========================================
        # LOAD DATAFRAME
        # ==========================================

        df = pd.read_csv(
            BytesIO(dataset_bytes)
        )

        # ==========================================
        # SCHEDULE
        # ==========================================

        schedule = {

            "frequency": (
                req.frequency
                or
                "daily"
            ),

            "time_utc": (
                req.time_utc
                or
                "00:00"
            ),

            "scheduled_at": (

                datetime.utcnow()
                .isoformat()

                + "Z"
            ),

            "active": True
        }

        # ==========================================
        # COSMOS JOB DOC
        # ==========================================

        cosmos_doc = {

            "job_id": req.job_id,

            "job_name": job_name,

            "thread_id": req.thread_id,

            "user_id": req.user_id,

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

            "schedule": schedule,

            # ======================================
            # DATASET
            # ======================================

            "dataset_name": (
                dataset_display_name
            ),

            "dataset_path": (
                dataset_path
            ),

            "onelake_path": (
                onelake_path
            ),

            "saved": True,

            # ======================================
            # AUTOML
            # ======================================

            "automl_registered": (
                automl_response
                is not None
            ),

            "automl_response": (
                automl_response
            ),

            # ======================================
            # PIPELINE FLAGS
            # ======================================

            "dq": False,

            "ner": False,

            "business_logic": False,

            "dashboard": False,

            "automl": (
                automl_response
                is not None
            ),

            # ======================================
            # METADATA
            # ======================================

            "metadata": {

                "rows": int(
                    len(df)
                ),

                "columns": int(
                    len(df.columns)
                ),

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
                        dataset_path
                    )
                }
            ]
        }

        # ==========================================
        # SAVE JOB
        # ==========================================

        cosmos_service.save_dataset(
            req.user_id,
            cosmos_doc
        )

        print(
            "✅ Saved to Cosmos"
        )

        # ==========================================
        # CREATED DATASET
        # ==========================================

        dataset_doc = {

            "job_id": req.job_id,

            "custom_table_name": (
                dataset_display_name
            ),

            "request_body": {

                "user_id": req.user_id,

                "job_id": req.job_id,

                "custom_table_name": (
                    dataset_display_name
                ),

                "column_mappings": [],

                "join_type": "INNER"
            },

            "file_path": (
                dataset_path
            ),

            "rows": int(
                len(df)
            ),

            "columns": list(
                df.columns
            ),

            "timestamp": (
                datetime.utcnow()
                .isoformat()
            )
        }

        cosmos_service.save_create_dataset(
            req.user_id,
            dataset_doc
        )

        print(
            "✅ Created dataset saved"
        )

        # ==========================================
        # UPDATE THREAD CONTEXT
        # ==========================================

        thread_service.update_context(

            req.thread_id,

            {

                "job_saved": True,

                "schedule": schedule,

                "latest_dataset_path": (
                    dataset_path
                ),

                "selected_dataset": {

                    **dataset,

                    "dataset_name": (
                        dataset_display_name
                    ),

                    "blob_path": (
                        dataset_path
                    ),

                    "onelake_path": (
                        onelake_path
                    ),

                    "saved": True,
                },
            },
        )

        # ==========================================
        # ASSISTANT MESSAGE
        # ==========================================

        assistant_message = (

            "✅ Job saved successfully.\n\n"

            "Artifacts Created:\n"

            "- Blob dataset saved\n"

            "- OneLake dataset saved\n"

            "- AutoML registered\n"

            "- Cosmos metadata saved"
        )

        # ==========================================
        # THREAD MESSAGE
        # ==========================================

        thread_service.add_message(

            thread_id=req.thread_id,

            role="assistant",

            content=assistant_message,

            message_type="completion",

            metadata={

                "dataset_name": (
                    dataset_display_name
                ),

                "blob_path": (
                    dataset_path
                ),

                "onelake_path": (
                    onelake_path
                ),

                "schedule": schedule,

                "automl_registered": (

                    automl_response
                    is not None
                ),
            },
        )

        save_job_response = {

            "success": True,

            "saved": True,

            "job_id": req.job_id,

            "job_name": job_name,

            "thread_id": req.thread_id,

            "dataset_name": dataset_display_name,

            "blob_path": dataset_path,

            "onelake_path": onelake_path,

            "schedule": schedule,

            "automl_registered": (
                automl_response is not None
            ),

            "automl_response": automl_response,

            "message": assistant_message
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="save_job",

            status="completed",

            request={

                "job_id": req.job_id,

                "user_id": req.user_id
            },

            response=save_job_response
        )

        # ==========================================
        # RESPONSE
        # ==========================================

        return save_job_response

    except Exception as e:

        error_response = {

            "status": "failed",

            "error": str(e),

            "job_id": req.job_id
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="save_job",

            status="failed",

            request={

                "job_id": req.job_id
            },

            response=error_response
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

# =========================================================
# RENAME JOB
# =========================================================

@app.post("/rename-job")
def rename_job(req: RenameJobRequest):

    try:

        print("\n🔁 RENAME JOB STARTED")

        job = cosmos_service.get_dataset(
            req.user_id,
            req.job_id
        )

        if not job:

            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )

        old_job_name = job.get(
            "job_name",
            ""
        )

        new_job_name = (
            req.new_name
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
            .strip()
        )

        # ==================================
        # UPDATE COSMOS
        # ==================================

        job["job_name"] = new_job_name
        job["thread_id"] = req.thread_id

        job["updated_at"] = (
            datetime.utcnow().isoformat()
            + "Z"
        )

        cosmos_service.update_dataset(
            req.user_id,
            job
        )

        print("✅ Cosmos updated")

        # ==================================
        # UPDATE THREAD
        # ==================================

        if req.thread_id:

            thread_service.update_context(

                req.thread_id,

                {
                    "job_name": new_job_name
                }
            )

            thread_service.add_message(

                thread_id=req.thread_id,

                role="assistant",

                content=(
                    f"Job renamed successfully.\n\n"
                    f"Old Name: {old_job_name}\n"
                    f"New Name: {new_job_name}"
                ),

                message_type="completion"
            )

            print("✅ Thread updated")

        rename_job_response = {

            "status": "success",

            "message": "Job renamed successfully",

            "job_id": req.job_id,

            "thread_id": req.thread_id,

            "old_job_name": old_job_name,

            "new_job_name": new_job_name,

            "next_actions": [

                {
                    "action": "rename_dataset",
                    "label": "Rename Dataset"
                },

                {
                    "action": "dashboard",
                    "label": "Generate Dashboard"
                }
            ]
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            action_type="rename_job",

            role="assistant",

            status="completed",

            request={

                "job_id": req.job_id,

                "old_job_name": old_job_name,

                "requested_name": req.new_name
            },

            response=rename_job_response
        )

        return rename_job_response
    
    except Exception as e:

        print(
            "❌ RENAME JOB ERROR:",
            str(e)
        )

        thread_service.add_action(

        thread_id=req.thread_id,

        role="assistant",

        action_type="rename_job",

        status="failed",

        request={

            "job_id": req.job_id,

            "requested_name": req.new_name
        },

        response={

            "status": "failed",

            "error": str(e)
        }
    )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.post("/rename-dataset")
def rename_dataset(req: RenameDatasetRequest):

    try:

        print("\n🔁 RENAME DATASET STARTED")

        # ==========================================
        # LOAD JOB
        # ==========================================

        job = cosmos_service.get_dataset(
            req.user_id,
            req.job_id
        )

        if not job:

            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )

        # ==========================================
        # OLD VALUES
        # ==========================================

        old_dataset_name = job.get(
            "dataset_name"
        )

        old_dataset_path = job.get(
            "dataset_path"
        )

        old_onelake_path = job.get(
            "onelake_path"
        )

        print("\n========== DEBUG ==========")
        print("Job ID:", req.job_id)
        print("Dataset Name:", old_dataset_name)
        print("Dataset Path:", old_dataset_path)
        print("OneLake Path:", old_onelake_path)
        print("===========================\n")

        # ==========================================
        # NEW DATASET NAME
        # ==========================================

        base_name = (
            req.new_name
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
            .strip()
        )

        if base_name.endswith("_dataset"):

            new_dataset_name = base_name

        else:

            new_dataset_name = (
                f"{base_name}_dataset"
            )

        new_dataset_file = (
            f"{new_dataset_name}.csv"
        )

        # ==========================================
        # NEW BLOB PATH
        # ==========================================

        new_dataset_path = (
            f"{req.user_id}/"
            f"{req.job_id}/"
            f"{new_dataset_file}"
        )

        # ==========================================
        # BLOB CONTAINER
        # ==========================================

        container = blob_service.get_container_client(
            V_DATASET_CONTAINER
        )

        print("\n📂 BLOBS FOUND:")

        blobs = list(
            container.list_blobs(
                name_starts_with=(
                    f"{req.user_id}/"
                    f"{req.job_id}/"
                )
            )
        )

        for b in blobs:
            print(b.name)

        # ==========================================
        # FALLBACK IF COSMOS PATH WRONG
        # ==========================================

        old_blob = container.get_blob_client(
            old_dataset_path
        )

        if not old_blob.exists():

            print(
                "⚠️ Cosmos dataset_path incorrect"
            )

            if not blobs:

                raise HTTPException(
                    status_code=404,
                    detail="No dataset blob found"
                )

            actual_blob_path = blobs[0].name

            print(
                "✅ Using actual blob:",
                actual_blob_path
            )

            old_dataset_path = (
                actual_blob_path
            )

            old_blob = (
                container.get_blob_client(
                    actual_blob_path
                )
            )

        # ==========================================
        # RENAME BLOB
        # ==========================================

        new_blob = container.get_blob_client(
            new_dataset_path
        )

        new_blob.start_copy_from_url(
            old_blob.url
        )

        props = new_blob.get_blob_properties()

        while props.copy.status == "pending":

            props = (
                new_blob.get_blob_properties()
            )

        if props.copy.status != "success":

            raise Exception(
                "Blob rename failed"
            )

        old_blob.delete_blob()

        print(
            "✅ Blob renamed"
        )

        # ==========================================
        # UPDATE COSMOS
        # ==========================================

        job["dataset_name"] = (
            new_dataset_name
        )

        job["thread_id"] = req.thread_id

        job["dataset_path"] = (
            new_dataset_path
        )

        # KEEP EXISTING ONELAKE PATH
        job["onelake_path"] = (
            old_onelake_path
        )

        job["updated_at"] = (
            datetime.utcnow().isoformat()
            + "Z"
        )

        cosmos_service.update_dataset(
            req.user_id,
            job
        )

        print(
            "✅ Cosmos updated"
        )

        # ==========================================
        # UPDATE CREATED DATASET
        # ==========================================

        cosmos_service.rename_created_dataset(

            user_id=req.user_id,

            job_id=req.job_id,

            new_dataset_name=new_dataset_name,

            new_dataset_path=new_dataset_path,

            new_onelake_path=old_onelake_path
        )

        print(
            "✅ Created dataset updated"
        )


       # ==========================================
        # UPDATE THREAD
        # ==========================================

        job["thread_id"] = req.thread_id

        if req.thread_id:

            thread_service.update_context(

                req.thread_id,

                {

                    "latest_dataset_path":
                        new_dataset_path,

                    "selected_dataset": {

                        "dataset_name":
                            new_dataset_name,

                        "blob_path":
                            new_dataset_path,

                        "onelake_path":
                            old_onelake_path
                    }
                }
            )

            thread_service.add_message(

                thread_id=req.thread_id,

                role="assistant",

                content=(

                    f"Dataset renamed successfully.\n\n"

                    f"Old Dataset: "
                    f"{old_dataset_name}\n"

                    f"New Dataset: "
                    f"{new_dataset_name}\n\n"

                    f"Blob Path:\n"
                    f"{new_dataset_path}\n\n"

                    f"OneLake Path:\n"
                    f"{old_onelake_path}"
                ),

                message_type="completion",

                metadata={

                    "job_id": req.job_id,

                    "old_dataset_name": old_dataset_name,

                    "new_dataset_name": new_dataset_name,

                    "blob_path": new_dataset_path,

                    "onelake_path": old_onelake_path
                }
            )

            print("✅ Thread updated")
        
        rename_dataset_response = {

            "status": "success",

            "message":
                "Dataset renamed successfully",

            "job_id":
                req.job_id,

            "old_dataset_name":
                old_dataset_name,

            "new_dataset_name":
                new_dataset_name,

            "blob_path":
                new_dataset_path,

            "onelake_path":
                old_onelake_path,

            "next_actions": [

                {
                    "action": "dq",
                    "label": "Apply Data Quality Rules"
                },

                {
                    "action": "business_logic",
                    "label": "Apply Business Logic"
                },

                {
                    "action": "dashboard",
                    "label": "Generate Dashboard"
                },

                {
                    "action": "automl",
                    "label": "Build AutoML Model"
                }
            ]
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="rename_dataset",

            status="completed",

            request={

                "job_id":
                    req.job_id,

                "old_dataset_name":
                    old_dataset_name,

                "requested_name":
                    req.new_name
            },

            response=rename_dataset_response
        )

        return rename_dataset_response
    except Exception as e:

        print(
            "❌ RENAME DATASET ERROR:",
            str(e)
        )
        thread_service.add_action(

        thread_id=req.thread_id,

        role="assistant",

        action_type="rename_dataset",

        status="failed",

        request={

            "job_id": req.job_id,

            "requested_name": req.new_name
        },

        response={

            "status": "failed",

            "error": str(e)
        }
    )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

# =========================
# GET JOBS
# =========================

@app.get("/jobs/{user_id}")
def get_jobs(user_id: str):

    try:

        jobs = cosmos_service.list_jobs(
            user_id
        )

        return {

            "status": "success",

            "jobs": jobs
        }

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )


# =========================
# GET JOB DETAILS
# =========================

@app.get("/jobs/{user_id}/{job_id}")
def get_job_details(
    user_id: str,
    job_id: str
):

    try:

        job = cosmos_service.get_dataset(
            user_id,
            job_id
        )

        if not job:

            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )

        return {

            "status": "success",

            "job": job
        }

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )


# =========================
# GENERATE POWERBI DASHBOARD
# =========================
@app.post("/generate_powerbi_dashboard")
def generate_dashboard(
    req: PowerBIDashboardRequest
):

    try:

        # =====================================
        # GENERATE DASHBOARD
        # =====================================

        result = generate_powerbi_dashboard(
            req.csv_blob,
            req.user_prompt
        )

        clean_result = sanitize_for_json(
            result
        )

        # =====================================
        # BUILD FINAL RESPONSE
        # (store exactly what frontend gets)
        # =====================================

        dashboard_response = {

            **clean_result,

            "action_type": "dashboard",

            "next_actions": [

                {
                    "action": "save_job",
                    "label": "Save Job"
                },

                {
                    "action": "automl",
                    "label": "Build AutoML Model"
                }
            ]
        }

        # =====================================
        # SAVE ASSISTANT MESSAGE
        # =====================================

        try:

            thread_service.add_message(

                thread_id=req.thread_id,

                role="assistant",

                message_type="completion",

                content="✅ Power BI Dashboard generated successfully.",

                metadata={

                    "dashboard_generated": True,

                    "total_kpis":
                        dashboard_response.get(
                            "total_kpis_discovered",
                            0
                        ),

                    "visual_count":
                        len(
                            dashboard_response.get(
                                "visuals",
                                []
                            )
                        )
                }
            )

            print(
                "✅ Dashboard message saved"
            )

        except Exception as e:

            print(
                "⚠️ Dashboard message save failed:",
                str(e)
            )

        # =====================================
        # SAVE ACTION
        # =====================================

        try:

            thread_service.add_action(

                thread_id=req.thread_id,

                role="assistant",

                action_type="dashboard",

                status="completed",

                request={

                    "job_id":
                        req.job_id,

                    "user_id":
                        req.user_id,

                    "csv_blob":
                        req.csv_blob,

                    "prompt":
                        req.user_prompt
                },

                response=dashboard_response
            )

            print(
                "✅ Dashboard action saved"
            )

        except Exception as e:

            print(
                "⚠️ Dashboard action save failed:",
                str(e)
            )

        # =====================================
        # UPDATE THREAD CONTEXT
        # =====================================

        try:

            thread_service.update_context(

                req.thread_id,

                {

                    "dashboard_completed":
                        True,

                    "latest_dashboard":
                        dashboard_response
                }
            )

            print(
                "✅ Thread context updated"
            )

        except Exception as e:

            print(
                "⚠️ Context update failed:",
                str(e)
            )

        # =====================================
        # UPDATE COSMOS
        # =====================================

        try:

            job_doc = cosmos_service.get_dataset(

                req.user_id,

                req.job_id
            )

            if job_doc:

                job_doc["dashboard"] = True

                job_doc["dashboard_result"] = (
                    dashboard_response
                )

                job_doc["thread_id"] = (
                    req.thread_id
                )

                job_doc["updated_at"] = (

                    datetime.utcnow().isoformat()
                    + "Z"
                )

                cosmos_service.update_dataset(

                    req.user_id,

                    job_doc
                )

                print(
                    "✅ Cosmos updated"
                )

        except Exception as e:

            print(
                "⚠️ Cosmos update failed:",
                str(e)
            )

        # =====================================
        # RETURN EXACT RESPONSE
        # =====================================

        return JSONResponse(

            content=dashboard_response
        )

    except Exception as e:

        dashboard_error = {

            "status": "failed",

            "error": str(e)
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="dashboard",

            status="failed",

            request={

                "job_id": req.job_id,

                "prompt": req.user_prompt
            },

            response=dashboard_error
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
# =========================
# AUTOML
# =========================

@app.post("/automl/run")
def run_automl(req: AutoMLRequest):

    try:

        # =====================================
        # START AUTOML
        # =====================================

        start_res = requests.post(

            "https://api.veriton.ai/api/service3/process_task_query_v",

            headers={
                "Content-Type":
                    "application/x-www-form-urlencoded"
            },

            data={

                "session_id":
                    req.session_id,

                "query":
                    req.query,

                "user_email":
                    req.user_email,
            },

        ).json()

        if start_res.get("status") != "started":

            raise Exception(
                "Failed to start AutoML"
            )

        job_id_ext = start_res.get(
            "job_id"
        )

        status_url = (

            "https://api.veriton.ai/api/service3/"
            f"process-task-query-status/{job_id_ext}"
        )

        # =====================================
        # POLL
        # =====================================

        while True:

            time.sleep(

                start_res.get(
                    "poll_every_seconds",
                    10
                )
            )

            poll_res = requests.get(

                status_url,

                params={
                    "user_email":
                        req.user_email
                }

            ).json()

            status = poll_res.get(
                "status"
            )

            # =====================================
            # SUCCESS
            # =====================================

            if status == "success":

                automl_response = {

                    **poll_res,

                    "action_type":
                        "automl",

                    "next_actions": [

                        {
                            "action":
                                "save_job",

                            "label":
                                "Save Job"
                        },

                        {
                            "action":
                                "dashboard",

                            "label":
                                "Generate Dashboard"
                        }
                    ]
                }

                # =====================================
                # SAVE USER MESSAGE
                # =====================================

                try:

                    thread_service.add_message(

                        thread_id=req.thread_id,

                        role="user",

                        content=req.query,

                        message_type="query",

                        metadata={

                            "action":
                                "automl"
                        }
                    )

                except Exception as e:

                    print(
                        "⚠️ User message save failed:",
                        str(e)
                    )

                # =====================================
                # SAVE ASSISTANT MESSAGE
                # =====================================

                try:

                    thread_service.add_message(

                        thread_id=req.thread_id,

                        role="assistant",

                        content=(
                            "✅ AutoML model build "
                            "completed successfully."
                        ),

                        message_type="completion",

                        metadata={

                            "job_id":
                                job_id_ext,

                            "action":
                                "automl"
                        }
                    )

                except Exception as e:

                    print(
                        "⚠️ Assistant message save failed:",
                        str(e)
                    )

                # =====================================
                # SAVE ACTION
                # =====================================

                try:

                    thread_service.add_action(

                        thread_id=req.thread_id,

                        role="assistant",

                        action_type="automl",

                        status="completed",

                        request={

                            "job_id":
                                req.job_id,

                            "user_id":
                                req.user_id,

                            "session_id":
                                req.session_id,

                            "query":
                                req.query,

                            "user_email":
                                req.user_email
                        },

                        response=automl_response
                    )

                    print(
                        "✅ AutoML action saved"
                    )

                except Exception as e:

                    print(
                        "⚠️ AutoML action save failed:",
                        str(e)
                    )

                # =====================================
                # UPDATE THREAD CONTEXT
                # =====================================

                try:

                    thread_service.update_context(

                        req.thread_id,

                        {

                            "automl_completed":
                                True,

                            "automl_job_id":
                                job_id_ext,

                            "latest_automl_result":
                                automl_response
                        }
                    )

                except Exception as e:

                    print(
                        "⚠️ Context update failed:",
                        str(e)
                    )

                # =====================================
                # UPDATE COSMOS
                # =====================================

                try:

                    job_doc = (

                        cosmos_service
                        .get_dataset(

                            req.user_id,

                            req.job_id
                        )
                    )

                    if job_doc:

                        job_doc[
                            "automl"
                        ] = True

                        job_doc[
                            "automl_result"
                        ] = automl_response

                        job_doc[
                            "thread_id"
                        ] = req.thread_id

                        job_doc[
                            "updated_at"
                        ] = (

                            datetime.utcnow()
                            .isoformat()

                            + "Z"
                        )

                        cosmos_service.update_dataset(

                            req.user_id,

                            job_doc
                        )

                        print(
                            "✅ Cosmos updated"
                        )

                except Exception as e:

                    print(
                        "⚠️ Cosmos update failed:",
                        str(e)
                    )

                # =====================================
                # RETURN EXACT RESPONSE
                # =====================================

                return automl_response

            # =====================================
            # FAILED
            # =====================================

            elif status == "failed":

                raise Exception(
                    "AutoML job failed"
                )

    except Exception as e:

        automl_error = {

            "status": "failed",

            "error": str(e)
        }

        thread_service.add_action(

            thread_id=req.thread_id,

            role="assistant",

            action_type="automl",

            status="failed",

            request={

                "query": req.query
            },

            response=automl_error
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


# =========================
# THREADS
# =========================

@app.post("/create-thread")
def create_thread(req: CreateThreadRequest):
    return thread_service.create_thread(
        user_id=req.user_id, job_id=req.job_id, title=req.title
    )


@app.get("/thread/{thread_id}")
def get_thread(thread_id: str):
    thread = thread_service.load_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return thread


@app.get("/threads/{user_id}/{job_id}")
def get_threads(user_id: str, job_id: str):
    return thread_service.list_threads(user_id, job_id)


# =========================
# CHAT
# =========================

@app.post("/chat")
async def chat(req: ChatRequest):

    try:

        # ==========================================
        # USER MESSAGE
        # ==========================================

        thread_service.add_message(
            thread_id=req.thread_id,
            role="user",
            content=req.message,
            message_type="text",
        )

        # ==========================================
        # LOAD THREAD
        # ==========================================

        thread = thread_service.load_thread(req.thread_id)

        if not thread:
            raise HTTPException(
                status_code=404,
                detail="Thread not found"
            )

        context = thread["context"]

        print("\n========== THREAD DEBUG ==========")
        print(json.dumps(context, indent=2))
        print("=================================\n")

        # =====================================================
        # PIPELINE FLOW
        # =====================================================

        pipeline_ctx = context.get("pipeline_creation", {})

        print("PIPELINE STATE:", pipeline_ctx)

        # =====================================================
        # STEP 0 - JOB SELECTION
        # =====================================================

        if (
            pipeline_ctx.get("active")
            and pipeline_ctx.get("step") == "select_jobs"
            and req.selected_jobs
            and len(req.selected_jobs) > 0
        ):
            available_jobs = pipeline_ctx.get("available_jobs", [])

            selected_job_details = [
                {
                    "job_id": job["job_id"],
                    "job_name": job.get("job_name")
                }
                for job in available_jobs
                if job["job_id"] in req.selected_jobs
            ]

            pipeline_ctx["selected_jobs"] = req.selected_jobs
            pipeline_ctx["selected_job_details"] = selected_job_details

            thread_service.add_action(
                thread_id=req.thread_id,
                role="user",
                action_type="pipeline_job_selection",
                status="completed",
                request={"selected_jobs": selected_job_details},
                response={}
            )

            pipeline_ctx["step"] = "pipeline_name"

            thread_service.update_context(
                req.thread_id,
                {"pipeline_creation": pipeline_ctx}
            )

            pipeline_response = {
                "status": "pipeline_name_required",
                "message": "Enter pipeline name."
            }

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="pipeline_wizard",
                status="waiting",
                request={"prompt": "Enter pipeline name"},
                response=pipeline_response
            )

            return pipeline_response

        # =====================================================
        # PIPELINE WIZARD
        # =====================================================

        if pipeline_ctx.get("active"):

            step = pipeline_ctx.get("step")

            # ==========================================
            # PIPELINE NAME
            # ==========================================

            if step == "pipeline_name":

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="user",
                    action_type="pipeline_name",
                    status="completed",
                    request={"pipeline_name": req.message},
                    response={}
                )

                pipeline_ctx["pipeline_name"] = req.message
                pipeline_ctx["step"] = "schedule_decision"

                thread_service.update_context(
                    req.thread_id,
                    {"pipeline_creation": pipeline_ctx}
                )

                pipeline_response = {
                    "status": "pipeline_schedule_decision_required",
                    "message": "Do you want to schedule this pipeline? (yes/no)"
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="pipeline_wizard",
                    status="waiting",
                    request={"prompt": "Do you want to schedule this pipeline?"},
                    response=pipeline_response
                )

                return pipeline_response

            # ==========================================
            # SCHEDULE DECISION
            # ==========================================

            elif step == "schedule_decision":

                decision = req.message.strip().lower()

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="user",
                    action_type="pipeline_schedule_decision",
                    status="completed",
                    request={"decision": decision},
                    response={}
                )

                if decision not in ["yes", "no"]:
                    error_response = {
                        "status": "failed",
                        "message": "Please answer yes or no."
                    }
                    return error_response

                # ======================================
                # NO SCHEDULE
                # ======================================

                if decision == "no":

                    pipeline_doc = {
                        "pipeline_id": generate_pipeline_id(),
                        "name": pipeline_ctx["pipeline_name"],
                        "created_at": datetime.utcnow().isoformat() + "Z",
                        "job_ids": pipeline_ctx["selected_job_details"],
                        "status": "SUCCESS",
                        "schedule": {
                            "active": False
                        }
                    }

                    cosmos_service.save_pipeline(req.user_id, pipeline_doc)

                    thread_service.update_context(
                        req.thread_id,
                        {"pipeline_creation": {"active": False}}
                    )

                    pipeline_response = {
                        "status": "success",
                        "message": "Pipeline created successfully.",
                        "pipeline_id": pipeline_doc["pipeline_id"],
                         "selected_jobs": pipeline_ctx.get(
                            "selected_job_details",
                            []
                        ),
                        "pipeline": pipeline_doc
                    }

                    thread_service.add_action(
                        thread_id=req.thread_id,
                        role="assistant",
                        action_type="pipeline",
                        status="completed",
                        request={
                            "pipeline_name": pipeline_ctx["pipeline_name"],
                            "selected_jobs":
                                pipeline_ctx.get(
                                    "selected_job_details",
                                    []
                                ),
                            "schedule": False
                        },
                        response=pipeline_response
                    )

                    thread_service.add_message(

                        thread_id=req.thread_id,

                        role="assistant",

                        content="Pipeline created successfully.",

                        message_type="completion",

                        metadata=pipeline_response
                )

                    return pipeline_response

                # ======================================
                # YES SCHEDULE
                # ======================================

                pipeline_ctx["step"] = "schedule_details"

                thread_service.update_context(
                    req.thread_id,
                    {"pipeline_creation": pipeline_ctx}
                )

                pipeline_response = {
                    "status": "pipeline_schedule_details_required",
                    "message": (
                        "Enter frequency, start date and time.\n\n"
                        "Example:\n"
                        "daily, 2026-06-15, 09:00"
                    )
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="pipeline_wizard",
                    status="waiting",
                    request={"prompt": "Enter frequency, start date and time"},
                    response=pipeline_response
                )

                return pipeline_response

            # ==========================================
            # SCHEDULE DETAILS
            # ==========================================

            elif step == "schedule_details":

                try:
                    parts = [p.strip() for p in req.message.split(",")]

                    if len(parts) != 3:
                        raise Exception()

                    frequency = parts[0]
                    start_date = parts[1]
                    time_utc = parts[2]

                    if frequency not in ["daily", "weekly", "monthly"]:
                        raise Exception()

                except Exception:
                    error_response = {
                        "status": "failed",
                        "message": "Format should be: daily, 2026-06-15, 09:00"
                    }

                    thread_service.add_action(
                        thread_id=req.thread_id,
                        role="assistant",
                        action_type="pipeline_wizard",
                        status="failed",
                        request={"schedule_input": req.message},
                        response=error_response
                    )

                    return error_response

                # ======================================
                # STORE USER INPUT
                # ======================================

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="user",
                    action_type="pipeline_schedule_details",
                    status="completed",
                    request={
                        "frequency": frequency,
                        "start_date": start_date,
                        "time": time_utc
                    },
                    response={}
                )

                pipeline_ctx["frequency"] = frequency
                pipeline_ctx["start_date"] = start_date
                pipeline_ctx["time_utc"] = time_utc

                # ======================================
                # CREATE PIPELINE
                # ======================================

                pipeline_doc = {
                    "pipeline_id": generate_pipeline_id(),
                    "name": pipeline_ctx["pipeline_name"],
                    "created_at": datetime.utcnow().isoformat() + "Z",
                    "job_ids": pipeline_ctx["selected_job_details"],
                    "status": "SUCCESS",
                    "last_run": None,
                    "description": "",
                    "last_run_started_at": None,
                    "last_run_result": None,
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                    "schedule": {
                        "frequency": frequency,
                        "time_utc": time_utc,
                        "start_date": start_date,
                        "active": True
                    }
                }

                cosmos_service.save_pipeline(req.user_id, pipeline_doc)

                thread_service.update_context(
                    req.thread_id,
                    {"pipeline_creation": {"active": False}}
                )

                pipeline_response = {
                    "status": "success",
                    "message": "Pipeline created successfully.",
                    "pipeline_id": pipeline_doc["pipeline_id"],
                    "pipeline": pipeline_doc,
                    "selected_jobs": pipeline_ctx.get(
                        "selected_job_details",
                        []
                    ),
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="pipeline",
                    status="completed",
                    request={
                        "pipeline_name": pipeline_ctx["pipeline_name"],
                        "selected_jobs": pipeline_ctx.get("selected_job_details", []),
                        "frequency": frequency,
                        "start_date": start_date,
                        "time": time_utc
                    },
                    response=pipeline_response
                )

                thread_service.add_message(
                    thread_id=req.thread_id,
                    role="assistant",
                    content="Pipeline created successfully.",
                    message_type="completion",
                    metadata=pipeline_response
                )

                return pipeline_response

        # ==========================================
        # DETECT INTENT
        # ==========================================

        result = orchestrator.detect_intent(req.message, context)

        # ==========================================
        # CONVERSATION
        # ==========================================

        if result["type"] == "conversation":

            thread_service.add_message(
                thread_id=req.thread_id,
                role="assistant",
                content=result["response"],
                message_type="text",
            )

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="conversation",
                status="completed",
                request={"message": req.message},
                response=result
            )

            return result

        intent = result["intent"]

        # =====================================================
        # DQ
        # =====================================================

        if intent == "dq":

            dataset = context.get("selected_dataset")

            if not dataset:
                error_response = {
                    "status": "failed",
                    "message": "Please upload dataset first"
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="dq",
                    status="failed",
                    request={
                        "prompt": req.message,
                        "message": req.message
                    },
                    response=error_response
                )

                return error_response

            response = dq_service.run(dataset["blob_path"])

            job_doc = cosmos_service.get_dataset(req.user_id, req.job_id)

            if job_doc:
                job_doc["dq"] = True
                cosmos_service.update_dataset(req.user_id, job_doc)

            thread_service.update_context(
                req.thread_id,
                {
                    "dq_completed": True,
                    "selected_dataset": {
                        **dataset,
                        "dq_applied": True
                    }
                },
            )

            thread_service.add_message(
                thread_id=req.thread_id,
                role="assistant",
                content=(
                    f"DQ completed successfully.\n\n"
                    f"Rules Applied: {response['rules_applied']}\n"
                    f"Issues Before: {len(response['issues_before'])}\n"
                    f"Issues After: {len(response['issues_after'])}"
                ),
                message_type="completion",
                metadata=response,
            )

            response["next_actions"] = [
                {"action": "ner", "label": "Run Name Entity Resolution"},
                {"action": "business_logic", "label": "Apply Business Logic"},
                {"action": "dashboard", "label": "Generate Power BI Dashboard"},
                {"action": "automl", "label": "Build AutoML Model"}
            ]

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="dq",
                status="completed",
                request={
                    "prompt": req.message,
                    "dataset": dataset["blob_path"],
                    "job_id": req.job_id,
                    "user_id": req.user_id
                },
                response=response
            )

            return response

        # =====================================================
        # NER
        # =====================================================

        elif intent == "ner":

            dataset = context.get("selected_dataset")

            if not dataset:
                error_response = {
                    "status": "failed",
                    "message": "Please upload or attach dataset first."
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="ner",
                    status="failed",
                    request={
                        "prompt": req.message,
                        "message": req.message
                    },
                    response=error_response
                )

                return error_response

            response = ner_service.run(dataset["blob_path"])

            job_doc = cosmos_service.get_dataset(req.user_id, req.job_id)

            if job_doc:
                job_doc["ner"] = True
                cosmos_service.update_dataset(req.user_id, job_doc)

            thread_service.update_context(
                req.thread_id,
                {
                    "ner_completed": True,
                    "selected_dataset": {
                        **dataset,
                        "ner_applied": True,
                        "entity_columns": response.get("entity_columns_created", []),
                    },
                },
            )

            thread_service.add_message(
                thread_id=req.thread_id,
                role="assistant",
                content="NER completed successfully",
                message_type="completion",
                metadata=response,
            )

            response["next_actions"] = [
                {"action": "business_logic", "label": "Apply Business Logic"},
                {"action": "dashboard", "label": "Generate Power BI Dashboard"},
                {"action": "automl", "label": "Build AutoML Model"}
            ]

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="ner",
                status="completed",
                request={
                    "prompt": req.message,
                    "dataset": dataset["blob_path"],
                    "job_id": req.job_id,
                    "user_id": req.user_id
                },
                response=response
            )

            return response

        # =====================================================
        # BUSINESS LOGIC
        # =====================================================

        elif intent == "business_logic":

            dataset = context.get("selected_dataset")

            if not dataset:
                error_response = {
                    "status": "failed",
                    "message": "Please upload or generate dataset first."
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="business_logic",
                    status="failed",
                    request={
                        "prompt": req.message,
                        "job_id": req.job_id,
                        "user_id": req.user_id
                    },
                    response=error_response
                )

                return error_response

            # ==========================================
            # RUN BUSINESS LOGIC
            # ==========================================

            response = business_logic_service.run(blob_path=dataset["blob_path"])

            if response["status"] == "warning":

                thread_service.add_message(
                    thread_id=req.thread_id,
                    role="assistant",
                    content=response["message"],
                    message_type="warning",
                    metadata=response
                )

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="business_logic",
                    status="completed",
                    request={"dataset": dataset["blob_path"]},
                    response=response
                )

                return response

            # ==========================================
            # UPDATE COSMOS
            # ==========================================

            job_doc = cosmos_service.get_dataset(req.user_id, req.job_id)

            if job_doc:
                job_doc["business_logic"] = True
                job_doc["business_rules"] = response.get("generated_rules", [])
                job_doc["generated_columns"] = response.get("generated_columns", [])
                cosmos_service.update_dataset(req.user_id, job_doc)

            # ==========================================
            # UPDATE THREAD CONTEXT
            # ==========================================

            thread_service.update_context(
                req.thread_id,
                {
                    "business_logic_completed": True,
                    "selected_dataset": {
                        **dataset,
                        "business_logic_applied": True,
                        "generated_columns": response.get("generated_columns", []),
                        "generated_rules": response.get("generated_rules", [])
                    },
                },
            )

            # ==========================================
            # THREAD MESSAGE
            # ==========================================

            thread_service.add_message(
                thread_id=req.thread_id,
                role="assistant",
                content=(
                    f"Business Logic applied successfully.\n\n"
                    f"AI Rules Generated: {response['rules_received']}\n"
                    f"Rules Applied: {response['rules_applied']}\n"
                    f"Generated Features: {len(response['generated_columns'])}\n"
                    f"Rows: {response['rows']}\n"
                    f"Columns: {response['columns']}"
                ),
                message_type="completion",
                metadata=response,
            )

            # ==========================================
            # NEXT ACTIONS
            # ==========================================

            response["next_actions"] = [
                {"action": "dashboard", "label": "Generate Power BI Dashboard"},
                {"action": "automl", "label": "Build AutoML Model"}
            ]

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="business_logic",
                status="completed",
                request={
                    "prompt": req.message,
                    "dataset": dataset["blob_path"],
                    "job_id": req.job_id,
                    "user_id": req.user_id
                },
                response=response
            )

            return response

        # =====================================================
        # PIPELINE
        # =====================================================

        elif intent == "pipeline":

            jobs = cosmos_service.list_jobs_for_pipeline(req.user_id)

            if not jobs:
                error_response = {
                    "status": "failed",
                    "message": "No eligible jobs found."
                }

                thread_service.add_action(
                    thread_id=req.thread_id,
                    role="assistant",
                    action_type="pipeline",
                    status="failed",
                    request={"prompt": req.message},
                    response=error_response
                )

                return error_response

            thread_service.update_context(
                req.thread_id,
                {
                    "pipeline_creation": {
                        "active": True,
                        "step": "select_jobs",
                        "selected_jobs": [],
                        "available_jobs": jobs,
                        "pipeline_name": None,
                        "frequency": None,
                        "start_date": None,
                        "time_utc": None
                    }
                }
            )

            pipeline_response = {
                "status": "pipeline_job_selection_required",
                "jobs": jobs
            }

            thread_service.add_action(
                thread_id=req.thread_id,
                role="assistant",
                action_type="pipeline_wizard",
                status="waiting",
                request={"prompt": "Select jobs for pipeline creation"},
                response=pipeline_response
            )

            return pipeline_response

        # =====================================================
        # FALLBACK
        # =====================================================

        error_response = {
            "status": "failed",
            "message": "Intent not implemented yet."
        }

        thread_service.add_action(
            thread_id=req.thread_id,
            role="assistant",
            action_type="chat",
            status="failed",
            request={"prompt": req.message},
            response=error_response
        )

        return error_response

    except Exception as e:

        print("❌ CHAT ERROR:", str(e))

        thread_service.add_message(
            thread_id=req.thread_id,
            role="assistant",
            content=str(e),
            message_type="error",
        )

        thread_service.add_action(
            thread_id=req.thread_id,
            role="assistant",
            action_type="chat",
            status="failed",
            request={"prompt": req.message},
            response={
                "status": "failed",
                "error": str(e)
            }
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
    

# =========================
# PIPELINES
# =========================

@app.get("/pipelines")
def get_pipelines(user_id: str):
    return pipeline_manager.list_pipelines(user_id)


@app.post("/pipelines/add-job")
def add_job_to_pipeline(user_id: str, pipeline_id: str, job_id: str):
    pipeline = pipeline_manager.get_pipeline(user_id, pipeline_id)
    if job_id not in pipeline["jobs"]:
        pipeline["jobs"].append(job_id)
    pipeline_manager.update_schedule(user_id, pipeline_id, pipeline.get("schedule"))
    return pipeline


# =========================
# UPLOAD DATASET (with job_id)
# =========================

from typing import Optional

@app.post("/datasets/upload")
async def upload_dataset(

    user_id: str = Form(...),

    job_id: str = Form(...),

    session_id: str = Form(...),

    user_email: str = Form(...),

    thread_id: str = Form(...),

    file: UploadFile = File(...),

    sheet_name: Optional[str] = Form(None)
):
    try:

        return await upload_service.upload_dataset(

            user_id=user_id,

            job_id=job_id,

            thread_id=thread_id,

            session_id=session_id,

            user_email=user_email,

            file=file,

            sheet_name=sheet_name
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


# =========================
# UPLOAD DATASET (no job_id)
# =========================


import uuid
import pandas as pd

from io import BytesIO
from datetime import datetime

from fastapi import (

    UploadFile,

    File,

    Form,

    HTTPException
)

from azure.storage.blob import (
    BlobServiceClient
)

from services.onelake_service import (
    OneLakeService
)

from services.cosmos_service import (
    CosmosService
)

from config import (

    V_CONNECTION_STRING,

    V_DATASET_CONTAINER
)


# =====================================================
# SERVICES
# =====================================================

blob_service = BlobServiceClient.from_connection_string(
    V_CONNECTION_STRING
)

onelake_service = OneLakeService()

cosmos_service = CosmosService()

def detect_header_row(df):

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
# GENERATE JOB ID
# =====================================================

def generate_job_id():

    # 32 character alphanumeric
    return uuid.uuid4().hex


# =====================================================
# DATASET UPLOAD API
# =====================================================

@app.post("/datasets/upload/nojob_id")
async def upload_dataset(

    user_id: str = Form(...),

    dataset: UploadFile = File(...),

    sheet_name: Optional[str] = Form(None),

    job_id: str = Form(None)
):

    try:

        # ==========================================
        # GENERATE JOB ID
        # ==========================================

        if not job_id:

            job_id = generate_job_id()

            print(
                f"🆕 Job ID Generated: "
                f"{job_id}"
            )

        print(
            f"🆕 Job ID Generated: "
            f"{job_id}"
        )

        # ==========================================
        # READ FILE
        # ==========================================

        contents = await dataset.read()

        filename = dataset.filename.lower()

        # ==========================================
        # GENERATE JOB NAME
        # ==========================================

        base_name = (
            dataset.filename
            .split(".")[0]
        )

        # ==========================================
        # JOB NAME
        # ==========================================

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

        print(
            f"📌 Job Name: {job_name}"
        )

        # ==========================================
        # LOAD DATAFRAME
        # ==========================================

        if filename.endswith(".csv"):

            df = pd.read_csv(
                BytesIO(contents)
            )

        elif (

            filename.endswith(".xlsx")

            or

            filename.endswith(".xls")
        ):

            excel_file = pd.ExcelFile(
                BytesIO(contents)
            )

            sheets = excel_file.sheet_names

            # ======================================
            # INVALID SHEET CHECK
            # ======================================

            if sheet_name and sheet_name not in sheets:


                return {

                    "status": "sheet_selection_required",


                    "message": (
                        "Please select a valid sheet."
                    ),

                    "job_id": job_id,

                    "sheets": sheets
                }

            print(
                f"📄 Available Sheets: {sheets}"
            )

            # ======================================
            # MULTIPLE SHEETS
            # ======================================

            if len(sheets) > 1 and not sheet_name:

                return {

                    "status":
                        "sheet_selection_required",

                    "message":
                        "This workbook contains multiple sheets. Please select one.",

                    "job_id":
                        job_id,

                    "file_name":
                        dataset.filename,

                    "sheets":
                        sheets
                }

            selected_sheet = (

                sheet_name

                if sheet_name

                else sheets[0]
            )

            print(
                f"📌 Selected Sheet: "
                f"{selected_sheet}"
            )

            print(
                f"Received Sheet Name: "
                f"{sheet_name}"
            )

            # ======================================
            # LOAD RAW SHEET
            # ======================================

            raw_df = pd.read_excel(

                BytesIO(contents),

                sheet_name=selected_sheet,

                header=None
            )

            header_row = detect_header_row(
                raw_df
            )

            print(
                f"📌 Detected Header Row: "
                f"{header_row}"
            )

            df = pd.read_excel(

                BytesIO(contents),

                sheet_name=selected_sheet,

                header=header_row
            )

            print(
                f"📊 Final Columns: "
                f"{list(df.columns)}"
            )

        elif filename.endswith(".parquet"):

            df = pd.read_parquet(
                BytesIO(contents)
            )

        else:

            raise Exception(
                f"Unsupported file type: "
                f"{filename}"
            )

        # ==========================================
        # CONVERT TO CSV
        # ==========================================

        csv_buffer = BytesIO()

        df.to_csv(

            csv_buffer,

            index=False
        )

        csv_buffer.seek(0)

        # ==========================================
        # FINAL BLOB PATH
        # ==========================================

        dataset_path = (

            f"{user_id}/"
            f"{job_id}/"
            f"{dataset_file_name}"
        )

        # ==========================================
        # SAVE TO BLOB
        # ==========================================

        blob_client = (

            blob_service
            .get_blob_client(

                container=V_DATASET_CONTAINER,

                blob=dataset_path
            )
        )

        blob_client.upload_blob(

            csv_buffer.getvalue(),

            overwrite=True
        )

        print(
            "✅ Uploaded to Blob"
        )

        # ==========================================
        # SAVE TO ONELAKE
        # ==========================================

        onelake_path = (

            onelake_service
            .upload_file(

                BytesIO(
                    csv_buffer.getvalue()
                ),

                user_id,

                job_id,

                dataset_file_name
            )
        )

        print(
            "✅ Uploaded to OneLake"
        )

        # ==========================================
        # COSMOS DOCUMENT
        # ==========================================
        cosmos_doc = {

            "job_id": job_id,

            "job_name": job_name,

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

            "source_type": "direct_upload",

            "dataset_path": dataset_path,

            "onelake_path": onelake_path,

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
                ),

                "sheet_name": (
                    selected_sheet
                    if filename.endswith((".xlsx", ".xls"))
                    else None
                ),

                "header_row": (
                    header_row
                    if filename.endswith((".xlsx", ".xls"))
                    else None
                )
            },

            # ======================================
            # SOURCES
            # ======================================

            "sources": [

                "direct_upload"
            ],

            # ======================================
            # RESULTS
            # ======================================

            "results": [

                {
                    "source": "direct_upload",

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

        cosmos_service.save_dataset(

            user_id,

            cosmos_doc
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

        cosmos_service.save_create_dataset(

            user_id,

            dataset_doc
        )

        print(
            "✅ Saved to Cosmos"
        )

        # ==========================================
        # RESPONSE
        # ==========================================

        return {

            "status": "success",

            "job_id": job_id,

            "job_name": job_name,

            "user_id": user_id,

           "dataset_name": dataset_display_name,

            "dataset_path": dataset_path,

            "onelake_path": onelake_path,

            "rows": len(df),

            "columns": len(df.columns),

            "column_names": (
                list(df.columns)
            ),

            "saved": True
        }

    except Exception as e:

        print(
            "❌ Dataset Upload Error"
        )

        print(str(e))

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

@app.get(
    "/thread/{thread_id}/conversation"
)
def get_thread_conversation(
    thread_id: str
):

    try:

        thread = (
            thread_service.load_thread(
                thread_id
            )
        )

        if not thread:

            raise HTTPException(

                status_code=404,

                detail="Thread not found"
            )

        actions = thread.get(
            "actions",
            []
        )

        conversation = []

        for idx, action in enumerate(actions):

            conversation.append({

                "turn_id":
                    str(idx + 1),

                "role":
                    action.get(
                        "role"
                    ),

                "action_type":
                    action.get(
                        "action_type"
                    ),

                "status":
                    action.get(
                        "status"
                    ),

                "request":
                    action.get(
                        "request",
                        {}
                    ),

                "response":
                    action.get(
                        "response",
                        {}
                    ),

                "timestamp":
                    action.get(
                        "timestamp"
                    )
            })

        return {

            "thread_id":
                thread_id,

            "conversation":
                conversation
        }

    except HTTPException:

        raise

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

# =========================
# STARTUP
# =========================

@app.on_event("startup")
def load_schedules():
    print("🔄 Loading schedules from storage...")
    for s in schedule_service.get_all_schedules():
        try:
            schedule_pipeline(
                run_pipeline_execution,
                s["user_id"],
                s["pipeline_id"],
                s["schedule"],
            )
        except Exception as e:
            print("⚠️ Failed to restore schedule:", str(e))


# =========================
# HEALTH
# =========================

@app.get("/health")
def health():
    return {"status": "running"}
