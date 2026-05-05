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


class RenameJobRequest(BaseModel):
    user_id: str
    job_id: str
    new_name: str


class RenameDatasetRequest(BaseModel):
    user_id: str
    job_id: str
    new_name: str


# =========================
# RUN PIPELINE
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

        # Save temp result
        temp_blob = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{req.user_id}/{job_id}/temp_result.json"
        )

        temp_blob.upload_blob(json.dumps(result, indent=2), overwrite=True)

        final_dataset = result.get("final_dataset")

        return {
            "status": "success",
            "job_id": job_id,
            "suggested_job_name": f"{result['data_model']['fact_table']}_job",

            "data_model": result.get("data_model"),
            "relationships": result.get("relationships"),
            "schemas": result.get("schemas"),
            "final_dataset": final_dataset,

            # 🔥 DOWNLOAD URL
            "download_url": f"/download/{req.user_id}/{job_id}/{final_dataset['dataset_name']}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# DOWNLOAD DATASET
# =========================
@app.get("/download/{user_id}/{job_id}/{file_name}")
def download_dataset(user_id: str, job_id: str, file_name: str):
    try:
        blob_client = pipeline_service.dataset_blob_service.get_blob_client(
            container=V_DATASET_CONTAINER,
            blob=f"{user_id}/{job_id}/{file_name}"
        )

        if not blob_client.exists():
            raise HTTPException(status_code=404, detail="File not found")

        stream = blob_client.download_blob()

        return StreamingResponse(
            stream.chunks(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={file_name}"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# SAVE JOB (FULL RESULT)
# =========================
@app.post("/save-job")
def save_job(req: SaveJobRequest):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        # get temp result
        temp_blob = container.get_blob_client(f"{req.user_id}/{req.job_id}/temp_result.json")

        if not temp_blob.exists():
            raise HTTPException(status_code=404, detail="Run result not found")

        result = json.loads(temp_blob.download_blob().readall())

        # 🔥 auto job name from result
        fact_table = result["data_model"]["fact_table"]
        job_name = f"{fact_table}_job"

        safe_name = job_name.lower().replace(" ", "_")

        final_blob = container.get_blob_client(
            f"{req.user_id}/{req.job_id}/{safe_name}.json"
        )

        if final_blob.exists():
            raise HTTPException(status_code=409, detail="Job already exists")

        job_data = {
            "job_id": req.job_id,
            "job_name": job_name,
            "user_id": req.user_id,
            "result": result
        }

        final_blob.upload_blob(json.dumps(job_data, indent=2))

        return {
            "status": "success",
            "job_name": job_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# RENAME JOB
# =========================
@app.post("/rename-job")
def rename_job(req: RenameJobRequest):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = container.list_blobs(name_starts_with=f"{req.user_id}/{req.job_id}/")

        current_blob = None

        for b in blobs:
            if b.name.endswith(".json") and "temp_result" not in b.name:
                current_blob = b.name
                break

        if not current_blob:
            raise HTTPException(status_code=404, detail="Job not found")

        old_blob = container.get_blob_client(current_blob)

        new_name_clean = req.new_name.strip().replace(" ", "_").lower()
        new_blob_path = f"{req.user_id}/{req.job_id}/{new_name_clean}.json"

        new_blob = container.get_blob_client(new_blob_path)

        # copy
        new_blob.start_copy_from_url(old_blob.url)

        # delete old
        old_blob.delete_blob()

        # update inside JSON
        data = json.loads(new_blob.download_blob().readall())
        data["job_name"] = req.new_name

        new_blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

        return {
            "status": "success",
            "new_name": req.new_name
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

        blobs = container.list_blobs(name_starts_with=f"{req.user_id}/{req.job_id}/")

        dataset_blob = None

        for b in blobs:
            if b.name.endswith(".csv"):
                dataset_blob = b.name
                break

        if not dataset_blob:
            raise HTTPException(status_code=404, detail="Dataset not found")

        old_blob = container.get_blob_client(dataset_blob)

        new_name_clean = req.new_name.strip().replace(" ", "_")
        new_blob_path = f"{req.user_id}/{req.job_id}/{new_name_clean}"

        new_blob = container.get_blob_client(new_blob_path)

        # copy
        new_blob.start_copy_from_url(old_blob.url)

        # delete old
        old_blob.delete_blob()

        return {
            "status": "success",
            "new_name": req.new_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# =========================
# GET JOBS
# =========================
@app.get("/jobs/{user_id}")
def get_jobs(user_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")
        blobs = container.list_blobs(name_starts_with=f"{user_id}/")

        jobs = []

        for blob in blobs:
            if blob.name.endswith(".json") and "temp_result" not in blob.name:
                data = json.loads(
                    container.get_blob_client(blob.name).download_blob().readall()
                )

                result = data.get("result", {})

                jobs.append({
                    "job_id": data.get("job_id"),
                    "job_name": data.get("job_name"),
                    "status": "success",
                    **result
                })

        return jobs

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# GET JOB DETAILS
# =========================
@app.get("/jobs/{user_id}/{job_id}")
def get_job_details(user_id: str, job_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = container.list_blobs(name_starts_with=f"{user_id}/{job_id}/")

        for blob in blobs:
            if blob.name.endswith(".json") and "temp_result" not in blob.name:
                data = json.loads(
                    container.get_blob_client(blob.name).download_blob().readall()
                )

                result = data.get("result", {})

                return {
                    "status": "success",
                    "job_id": job_id,
                    "job_name": data.get("job_name"),
                    **result
                }

        raise HTTPException(status_code=404, detail="Job not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "running"}
