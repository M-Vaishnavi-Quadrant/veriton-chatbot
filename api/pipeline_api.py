from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.pipeline_service import PipelineService
from services.conversation_service import ConversationService
from services.pipeline_manager import PipelineManager
from services.scheduler_service import SchedulerService

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

conversation_service = ConversationService(pipeline_manager)

router = APIRouter()


# =========================
# REQUEST MODELS
# =========================
class RunRequest(BaseModel):
    prompt: str
    user_id: str


class ConversationRequest(BaseModel):
    session_id: str
    user_input: str


class RunSavedRequest(BaseModel):
    pipeline_name: str
    user_id: str


# =========================
# 1. RUN PIPELINE
# =========================
@router.post("/run")
def run_pipeline(req: RunRequest):

    try:
        result = pipeline_service.run(
            prompt=req.prompt,
            user_id=req.user_id
        )

        job_id = result["pipeline_metadata"]["job_id"]
        result["job_id"] = job_id

        # start conversation
        conv = conversation_service.start(
            prompt=req.prompt,
            user_id=req.user_id
        )

        result["conversation"] = conv

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# 2. CONVERSATION FLOW
# =========================
@router.post("/conversation")
def handle_conversation(req: ConversationRequest):

    try:
        return conversation_service.handle(
            session_id=req.session_id,
            user_input=req.user_input
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# 3. RUN SAVED PIPELINE
# =========================
@router.post("/run-saved")
def run_saved(req: RunSavedRequest):

    try:
        return pipeline_manager.run_saved_pipeline(
            pipeline_name=req.pipeline_name,
            user_id=req.user_id
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# 4. LIST PIPELINES
# =========================
@router.get("/pipelines/{user_id}")
def list_pipelines(user_id: str):

    try:
        container = pipeline_manager.blob.get_container_client("pipelines")

        pipelines = []

        for blob in container.list_blobs(name_starts_with=f"{user_id}/"):
            name = blob.name.split("/")[-1].replace(".json", "")
            pipelines.append(name)

        return {"pipelines": pipelines}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# 5. DELETE PIPELINE
# =========================
@router.delete("/pipelines/{user_id}/{pipeline_name}")
def delete_pipeline(user_id: str, pipeline_name: str):

    try:
        container = pipeline_manager.blob.get_container_client("pipelines")

        path = f"{user_id}/{pipeline_name}.json"

        container.delete_blob(path)

        return {"message": "Pipeline deleted"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/jobs/{user_id}")
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
    
@router.get("/jobs/{user_id}/{job_id}")
def get_job_details(user_id: str, job_id: str):

    try:
        blob_client = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{user_id}/{job_id}/result.json"
        )

        if not blob_client.exists():
            raise HTTPException(status_code=404, detail="Job not found")

        data = blob_client.download_blob().readall()

        import json
        return json.loads(data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/jobs-summary/{user_id}")
def get_jobs_summary(user_id: str):

    try:
        container = pipeline_service.blob_service.get_container_client("jobs")

        blobs = container.list_blobs(name_starts_with=f"{user_id}/")

        jobs = {}

        import json

        for blob in blobs:
            if blob.name.endswith("result.json"):

                parts = blob.name.split("/")
                job_id = parts[1]

                blob_client = pipeline_service.blob_service.get_blob_client(
                    container="jobs",
                    blob=blob.name
                )

                data = json.loads(blob_client.download_blob().readall())

                jobs[job_id] = {
                    "job_id": job_id,
                    "fact_table": data["data_model"]["fact_table"],
                    "rows": data["final_dataset"]["rows"],
                    "columns": data["final_dataset"]["columns"]
                }

        return list(jobs.values())

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
