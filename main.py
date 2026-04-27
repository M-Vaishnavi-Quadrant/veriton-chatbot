from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import uuid
import re

from azure.storage.blob import BlobServiceClient

from services.pipeline_service import PipelineService
from services.pipeline_manager import PipelineManager
from services.scheduler_service import SchedulerService
from config import BLOB_CONN_STR

def extract_pipeline_name(user_input: str):
    match = re.search(r"run (?:this )?pipeline (.+)", user_input.lower())
    if match:
        return match.group(1).strip()
    return None


app = FastAPI(title="Auto Pipeline System")

# =========================
# INIT SERVICES
# =========================
pipeline_service = PipelineService()
scheduler_service = SchedulerService()

pipeline_manager = PipelineManager(
    blob_service=pipeline_service.blob_service,
    scheduler=scheduler_service,
    pipeline_service=pipeline_service
)


# =========================
# REQUEST MODELS
# =========================
class PromptRequest(BaseModel):
    user_id: str
    prompt: str
    job_id: Optional[str] = None


class RunSavedRequest(BaseModel):
    user_id: str
    user_input: str

# =========================
# RUN + AUTO SAVE PIPELINE
# =========================
@app.post("/run-pipeline")
def run_pipeline(req: PromptRequest):
    try:
        # ensure job_id
        job_id = req.job_id or str(uuid.uuid4())

        # run pipeline
        result = pipeline_service.run(
            prompt=req.prompt,
            user_id=req.user_id,
            job_id=job_id
        )

        # generate pipeline name
        pipeline_name = f"pipeline_{str(uuid.uuid4())[:8]}"

        metadata = result.get("pipeline_metadata")

        # save pipeline
        pipeline_manager.create_pipeline(
            pipeline_name=pipeline_name,
            prompt=metadata["prompt"],
            user_id=metadata["user_id"],
            schedule=None
        )

        # ✅ FIX 1: store pipeline_name in result
        result["pipeline_metadata"]["pipeline_name"] = pipeline_name

        # ✅ FIX 2: save result.json
        import json
        blob_client = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{req.user_id}/{job_id}/result.json"
        )

        blob_client.upload_blob(
            json.dumps(result, indent=2),
            overwrite=True
        )

        # extract full model output
        model_data = result.get("data_model", {})
        relationships = result.get("relationships", [])
        schemas = result.get("schemas", {})

        return {
            "status": "success",
            "pipeline_name": pipeline_name,
            "job_id": job_id,
            "message": f"✅ Pipeline saved successfully as '{pipeline_name}'",

            "data_model": model_data,
            "relationships": relationships,
            "schemas": schemas,
            "final_dataset": result.get("final_dataset"),

            # download endpoint
            "download_url": f"/download/{req.user_id}/{job_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# RUN SAVED PIPELINE
# =========================
@app.post("/run-saved-pipeline")
def run_saved(req: RunSavedRequest):
    try:
        pipeline_name = extract_pipeline_name(req.user_input)

        if not pipeline_name:
            raise HTTPException(
                status_code=400,
                detail="❌ Could not extract pipeline name from input"
            )

        return pipeline_manager.run_saved_pipeline(
            pipeline_name=pipeline_name,
            user_id=req.user_id
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# DOWNLOAD DATASET FROM BLOB
# =========================
@app.get("/download/{user_id}/{job_id}")
def download_dataset(user_id: str, job_id: str):
    try:
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

        container = blob_service.get_container_client("finaldataset")

        blob_path = f"{user_id}/{job_id}/final_dataset.csv"

        blob_client = container.get_blob_client(blob_path)

        if not blob_client.exists():
            raise HTTPException(status_code=404, detail="Dataset not found")

        stream = blob_client.download_blob()

        return StreamingResponse(
            stream.chunks(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=final_dataset_{job_id}.csv"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# LIST PIPELINES
# =========================
@app.get("/pipelines/{user_id}")
def list_pipelines(user_id: str):
    try:
        container = pipeline_manager.blob.get_container_client("pipelines")

        pipelines = [
            blob.name.split("/")[-1].replace(".json", "")
            for blob in container.list_blobs(name_starts_with=f"{user_id}/")
        ]

        return {"pipelines": pipelines}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# DELETE PIPELINE
# =========================
@app.delete("/pipelines/{user_id}/{pipeline_name}")
def delete_pipeline(user_id: str, pipeline_name: str):
    try:
        container = pipeline_manager.blob.get_container_client("pipelines")

        path = f"{user_id}/{pipeline_name}.json"

        container.delete_blob(path)

        return {"message": "Pipeline deleted"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{user_id}")
def get_jobs(user_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = container.list_blobs(name_starts_with=f"{user_id}/")

        jobs = set()

        for blob in blobs:
            parts = blob.name.split("/")
            if len(parts) >= 2:
                jobs.add(parts[1])  # job_id

        return {
            "user_id": user_id,
            "jobs": list(jobs)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/jobs/{user_id}/{job_id}")
def get_job_details(user_id: str, job_id: str):
    try:
        blob_client = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{user_id}/{job_id}/result.json"
        )

        if not blob_client.exists():
            raise HTTPException(status_code=404, detail="Job not found")

        import json
        data = blob_client.download_blob().readall()

        result = json.loads(data)

        return {
            "job_id": job_id,

            # ✅ NEW
            "pipeline_name": result["pipeline_metadata"].get("pipeline_name"),

            "data_model": result.get("data_model"),
            "relationships": result.get("relationships"),
            "schemas": result.get("schemas"),
            "final_dataset": result.get("final_dataset"),

            # ✅ NEW
            "download_url": f"/download/{user_id}/{job_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/jobs-summary/{user_id}")
def get_jobs_summary(user_id: str):
    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = container.list_blobs(name_starts_with=f"{user_id}/")

        import json
        results = []

        for blob in blobs:
            if blob.name.endswith("result.json"):
                blob_client = pipeline_service.blob_service.get_blob_client(
                    container="jobs",
                    blob=blob.name
                )

                data = json.loads(blob_client.download_blob().readall())

                results.append({
                        "job_id": data["pipeline_metadata"]["job_id"],

                        # ✅ NEW
                        "pipeline_name": data["pipeline_metadata"].get("pipeline_name"),

                        "fact_table": data["data_model"]["fact_table"],
                        "rows": data["final_dataset"]["rows"],
                        "columns": data["final_dataset"]["columns"],

                        # ✅ NEW
                        "download_url": f"/download/{user_id}/{data['pipeline_metadata']['job_id']}"
                    })

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "running"}
