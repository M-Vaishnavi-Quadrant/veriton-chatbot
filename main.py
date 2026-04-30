from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import uuid
import json

from azure.storage.blob import BlobServiceClient

from services.pipeline_service import PipelineService
from config import BLOB_CONN_STR, V_CONNECTION_STRING, V_DATASET_CONTAINER

app = FastAPI(title="Job Execution System")

pipeline_service = PipelineService()

# =========================
# REQUEST MODELS
# =========================
class RunRequest(BaseModel):
    user_id: str
    prompt: str
    job_id: Optional[str] = None


class SaveJobRequest(BaseModel):
    user_id: str
    job_id: str
    job_name: str


class RenameDatasetRequest(BaseModel):
    user_id: str
    job_id: str
    old_name: str
    new_name: str


# =========================
# RUN (NO SAVE)
# =========================
@app.post("/run")
def run(req: RunRequest):
    try:
        job_id = req.job_id or str(uuid.uuid4())

        result = pipeline_service.run(
            prompt=req.prompt,
            user_id=req.user_id,
            job_id=job_id
        )

        return {
            "status": "success",
            "job_id": job_id,

            # 👇 suggested name
            "suggested_job_name": f"{result['data_model']['fact_table']}_job",

            "data_model": result.get("data_model"),
            "relationships": result.get("relationships"),
            "schemas": result.get("schemas"),

            # 🔥 IMPORTANT: includes dataset_name + dataset_path
            "final_dataset": result.get("final_dataset")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# SAVE JOB (WITH DATASET)
# =========================
@app.post("/save-job")
def save_job(req: SaveJobRequest):
    try:
        safe_name = req.job_name.strip().replace(" ", "_").lower()

        # =========================
        # FIND DATASET AUTOMATICALLY
        # =========================
        dataset_container = pipeline_service.dataset_blob_service.get_container_client(V_DATASET_CONTAINER)

        blobs = dataset_container.list_blobs(name_starts_with=f"{req.user_id}/{req.job_id}/")

        dataset_blob = None

        for b in blobs:
            if b.name.endswith(".csv"):
                dataset_blob = b.name
                break

        if not dataset_blob:
            raise HTTPException(status_code=404, detail="Dataset not found for this job")

        dataset_name = dataset_blob.split("/")[-1]

        # =========================
        # SAVE JOB
        # =========================
        blob_path = f"{req.user_id}/{req.job_id}/{safe_name}.json"

        blob = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=blob_path
        )

        if blob.exists():
            raise HTTPException(status_code=409, detail="Job already exists")

        job_data = {
            "job_id": req.job_id,
            "job_name": req.job_name,
            "user_id": req.user_id,
            "dataset_name": dataset_name,
            "dataset_path": dataset_blob
        }

        blob.upload_blob(json.dumps(job_data, indent=2), overwrite=False)

        return {
            "status": "success",
            "job_name": req.job_name,
            "dataset_name": dataset_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# RENAME DATASET
# =========================
@app.post("/rename-dataset")
def rename_dataset(req: RenameDatasetRequest):
    try:
        blob_service = BlobServiceClient.from_connection_string(V_CONNECTION_STRING)
        container = blob_service.get_container_client(V_DATASET_CONTAINER)

        old_blob_path = f"{req.user_id}/{req.job_id}/{req.old_name}"
        new_blob_path = f"{req.user_id}/{req.job_id}/{req.new_name}"

        old_blob = container.get_blob_client(old_blob_path)
        new_blob = container.get_blob_client(new_blob_path)

        if not old_blob.exists():
            raise HTTPException(status_code=404, detail="Dataset not found")

        # copy
        new_blob.start_copy_from_url(old_blob.url)

        # delete old
        old_blob.delete_blob()

        # update job metadata
        jobs_container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = jobs_container.list_blobs(name_starts_with=f"{req.user_id}/{req.job_id}/")

        for b in blobs:
            job_blob = jobs_container.get_blob_client(b.name)
            data = json.loads(job_blob.download_blob().readall())

            data["dataset_name"] = req.new_name
            data["dataset_path"] = new_blob_path

            job_blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

        return {
            "status": "success",
            "new_name": req.new_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# LIST JOBS
# =========================
@app.get("/jobs/{user_id}")
def get_jobs(user_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")
        blobs = container.list_blobs(name_starts_with=f"{user_id}/")

        jobs = []

        for blob in blobs:
            if blob.name.endswith(".json"):
                blob_client = container.get_blob_client(blob.name)
                data = json.loads(blob_client.download_blob().readall())

                jobs.append({
                    "job_id": data.get("job_id"),
                    "job_name": data.get("job_name"),
                    "dataset_name": data.get("dataset_name"),
                    "dataset_path": data.get("dataset_path")
                })

        return jobs

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# JOB DETAILS
# =========================
@app.get("/jobs/{user_id}/{job_id}")
def get_job_details(user_id: str, job_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")
        blobs = container.list_blobs(name_starts_with=f"{user_id}/{job_id}/")

        for blob in blobs:
            if blob.name.endswith(".json"):
                blob_client = container.get_blob_client(blob.name)
                data = json.loads(blob_client.download_blob().readall())

                return data

        raise HTTPException(status_code=404, detail="Job not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "running"}
