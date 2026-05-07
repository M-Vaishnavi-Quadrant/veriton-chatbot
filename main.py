from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse,JSONResponse
from pydantic import BaseModel
from typing import Optional
import uuid
import json

from azure.storage.blob import BlobServiceClient

from services.onelake_service import OneLakeService
from services.pipeline_service import PipelineService
from config import BLOB_CONN_STR, V_CONNECTION_STRING, V_DATASET_CONTAINER
# from services.onelake_service import OneLakeService

from services.powerbi_service import (
    generate_powerbi_dashboard,
    sanitize_for_json
)

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


class PowerBIDashboardRequest(BaseModel):
    csv_blob: str     # e.g. "folder/myfile.csv"
    user_prompt: str 


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

        # =========================
        # STEP 1: READ TEMP RESULT
        # =========================
        temp_blob = container.get_blob_client(f"{req.user_id}/{req.job_id}/temp_result.json")

        if not temp_blob.exists():
            raise HTTPException(status_code=404, detail="Run result not found")

        result = json.loads(temp_blob.download_blob().readall())

        # =========================
        # STEP 2: GET DATASET INFO
        # =========================
        final_dataset = result.get("final_dataset", {})

        dataset_name = final_dataset.get("dataset_name")
        dataset_path = final_dataset.get("dataset_path")

        if not dataset_name or not dataset_path:
            raise HTTPException(status_code=400, detail="Dataset info missing")

        # =========================
        # STEP 3: VERIFY DATASET IN BLOB
        # =========================
        dataset_blob = pipeline_service.dataset_blob_service.get_blob_client(
            container=V_DATASET_CONTAINER,
            blob=dataset_path
        )

        if not dataset_blob.exists():
            raise HTTPException(status_code=404, detail="Dataset not found in dataset container")

        # =========================
        # STEP 4: ENSURE DATASET IN ONELAKE
        # =========================
        try:
            # Try upload (overwrite safe)
            pipeline_service.onelake.upload_file(
                file_buffer=dataset_blob.download_blob().readall(),
                user_id=req.user_id,
                job_id=req.job_id,
                dataset_name=dataset_name
            )
        except Exception as e:
            print("OneLake sync skipped:", str(e))

        # =========================
        # STEP 5: AUTO GENERATE JOB NAME
        # =========================
        fact_table = result.get("data_model", {}).get("fact_table", "job")
        job_name = f"{fact_table}_job"

        safe_name = job_name.lower().replace(" ", "_")

        final_blob = container.get_blob_client(
            f"{req.user_id}/{req.job_id}/{safe_name}.json"
        )

        if final_blob.exists():
            raise HTTPException(status_code=409, detail="Job already exists")

        # =========================
        # STEP 6: SAVE FULL RESULT
        # =========================
        job_data = {
            "job_id": req.job_id,
            "job_name": job_name,
            "user_id": req.user_id,
            "result": result
        }

        final_blob.upload_blob(json.dumps(job_data, indent=2))

        return {
            "status": "success",
            "job_name": job_name,
            "dataset_name": dataset_name
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

        # ✅ Read existing data safely
        data = json.loads(old_blob.download_blob().readall())

        # ✅ Update job name
        data["job_name"] = req.new_name

        new_name_clean = req.new_name.strip().replace(" ", "_").lower()
        new_blob_path = f"{req.user_id}/{req.job_id}/{new_name_clean}.json"

        new_blob = container.get_blob_client(new_blob_path)

        # ✅ Save updated JSON first
        new_blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

        # ✅ Delete old blob
        old_blob.delete_blob()

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
        print("\n🔁 RENAME DATASET STARTED")

        # =========================
        # STEP 1: GET JOB JSON
        # =========================
        jobs_container = pipeline_service.blob_service.get_container_client("jobs")

        job_blob = None
        for b in jobs_container.list_blobs(
            name_starts_with=f"{req.user_id}/{req.job_id}/"
        ):
            if b.name.endswith(".json") and "temp_result" not in b.name:
                job_blob = jobs_container.get_blob_client(b.name)
                break

        if not job_blob:
            raise HTTPException(status_code=404, detail="Job not found")

        data = json.loads(job_blob.download_blob().readall())

        # =========================
        # STEP 2: GET DATASET INFO
        # =========================
        result = data.get("result", data)
        final_dataset = result.get("final_dataset", {})

        old_dataset_name = final_dataset.get("dataset_name")
        old_blob_path = final_dataset.get("dataset_path")
        onelake_path = final_dataset.get("onelake_path")  # 🔥 KEEP SAME

        if not old_dataset_name or not old_blob_path:
            raise HTTPException(status_code=404, detail="Dataset info missing")

        print("OLD DATASET:", old_dataset_name)
        print("OLD BLOB PATH:", old_blob_path)

        # =========================
        # STEP 3: PREPARE NEW NAME
        # =========================
        new_name_clean = req.new_name.strip().replace(" ", "_")

        if not new_name_clean.endswith(".csv"):
            new_name_clean += ".csv"

        new_blob_path = f"{req.user_id}/{req.job_id}/{new_name_clean}"

        print("NEW DATASET:", new_name_clean)
        print("NEW BLOB PATH:", new_blob_path)

        # =========================
        # STEP 4: BLOB RENAME (SAFE)
        # =========================
        blob_service = BlobServiceClient.from_connection_string(V_CONNECTION_STRING)
        container = blob_service.get_container_client(V_DATASET_CONTAINER)

        old_blob = container.get_blob_client(old_blob_path)

        if not old_blob.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Dataset blob not found: {old_blob_path}"
            )

        if old_dataset_name != new_name_clean:
            new_blob = container.get_blob_client(new_blob_path)

            print("📦 Copying blob...")

            copy = new_blob.start_copy_from_url(old_blob.url)

            props = new_blob.get_blob_properties()
            while props.copy.status == "pending":
                props = new_blob.get_blob_properties()

            if props.copy.status != "success":
                raise Exception("Blob copy failed")

            print("✅ Copy completed")

            old_blob.delete_blob()
            print("🗑️ Old blob deleted")

        else:
            print("⚠️ Same name — skipping blob rename")

        # =========================
        # STEP 5: ONE LAKE (SKIP)
        # =========================
        print("ℹ️ Skipping OneLake rename (not supported reliably)")

        # =========================
        # STEP 6: UPDATE JOB JSON
        # =========================
        if "result" in data and "final_dataset" in data["result"]:
            data["result"]["final_dataset"]["dataset_name"] = new_name_clean
            data["result"]["final_dataset"]["dataset_path"] = new_blob_path
            data["result"]["final_dataset"]["onelake_path"] = onelake_path  # 🔥 unchanged
        else:
            data["final_dataset"]["dataset_name"] = new_name_clean
            data["final_dataset"]["dataset_path"] = new_blob_path
            data["final_dataset"]["onelake_path"] = onelake_path  # 🔥 unchanged

        job_blob.upload_blob(json.dumps(data, indent=2), overwrite=True)

        print("💾 Job JSON updated")

        return {
            "status": "success",
            "old_dataset_name": old_dataset_name,
            "new_dataset_name": new_name_clean,
            "dataset_path": new_blob_path,
            "onelake_path": onelake_path
        }

    except Exception as e:
        print("❌ ERROR:", str(e))
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

                dataset_name = result.get("final_dataset", {}).get("dataset_name")

                jobs.append({
                    "job_id": data.get("job_id"),
                    "job_name": data.get("job_name"),
                    "status": "success",
                    **result,

                    # 🔥 NEW
                    "download_url": f"/download/{user_id}/{data.get('job_id')}/{dataset_name}"
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

                dataset_name = result.get("final_dataset", {}).get("dataset_name")

                return {
                    "status": "success",
                    "job_id": job_id,
                    "job_name": data.get("job_name"),
                    **result,

                    # 🔥 NEW
                    "download_url": f"/download/{user_id}/{job_id}/{dataset_name}"
                }

        raise HTTPException(status_code=404, detail="Job not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate_powerbi_dashboard")
def generate_dashboard(req: PowerBIDashboardRequest):
   
    try:
        result = generate_powerbi_dashboard(req.csv_blob, req.user_prompt)
        return JSONResponse(content=sanitize_for_json(result))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "running"}
