from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse,JSONResponse
from pydantic import BaseModel
from typing import Optional
import uuid
import json

from azure.storage.blob import BlobServiceClient
import requests
import re

from services.pipeline_service import PipelineService
from config import BLOB_CONN_STR, V_CONNECTION_STRING, V_DATASET_CONTAINER
# from services.onelake_service import OneLakeService

from services.thread_service import ThreadService

thread_service = ThreadService()

from services.powerbi_service import (
    generate_powerbi_dashboard,
    sanitize_for_json
)

from services.pipeline_manager import PipelineManager
from services.schedule_service import ScheduleService
from collections import defaultdict
from services.scheduler_service import schedule_pipeline

pipeline_manager = PipelineManager()

# chat state (in-memory for now)
state_store = defaultdict(dict)
app = FastAPI(title="Job Execution System")

pipeline_service = PipelineService()
schedule_service = ScheduleService()

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
    dataset: str
    onelake_path: str   # 🔥 IMPORTANT
    session_id: str
    user_email: str


class RenameJobRequest(BaseModel):
    user_id: str
    job_id: str
    new_name: str


class RenameDatasetRequest(BaseModel):
    user_id: str
    job_id: str
    new_name: str


class PowerBIDashboardRequest(BaseModel):
    csv_blob: str
    user_prompt: str
    user_id: str       # 🔥 ADD
    job_id: str        # 🔥 ADD

class AutoMLRequest(BaseModel):
    user_id: str
    job_id: str
    session_id: str
    user_email: str
    query: str

class ChatRequest(BaseModel):
    user_id: str
    job_id: str
    message: str
    selected_jobs: list[str] = []


def extract_pipeline_name(message: str):
    """
    Extract pipeline name from flexible input.
    Example:
    - "sales pipeline daily 10:00" → "sales_pipeline"
    - "orders weekly 2 11:00" → "orders"
    """

    message = message.lower()

    keywords = ["daily", "weekly", "monthly", "no"]

    words = message.split()

    filtered = []

    for w in words:
        # stop at schedule keywords
        if w in keywords:
            break

        # stop at time
        if re.match(r"\d{1,2}:\d{2}", w):
            break

        filtered.append(w)

    return "_".join(filtered) if filtered else "pipeline"

def parse_pipeline_prompt(message: str):

    import re

    message = message.lower().strip()

    # normalize bad inputs
    message = message.replace(";", ":")
    message = message.replace(" at ", " ")

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

    schedule = {
        "type": freq,
        "hour": hour,
        "minute": minute
    }

    if freq == "weekly":

        cleaned = re.sub(r"\d{1,2}:\d{2}", "", message)

        day_match = re.search(r"\b[0-6]\b", cleaned)

        if day_match:
            schedule["day"] = day_match.group()

    if freq == "monthly":

        # 🔥 remove time first
        cleaned = re.sub(r"\d{1,2}:\d{2}", "", message)

        # now extract day
        day_match = re.search(r"\b(0?[1-9]|[12][0-9]|3[01])\b", cleaned)

        if day_match:
            schedule["day"] = str(int(day_match.group()))  # normalize (02 → 2)

    return {
        "pipeline_name": pipeline_name,
        "schedule": schedule
    }


# =========================
# RUN PIPELINE
# =========================
@app.post("/run")
def run(req: RunRequest):
    try:
        print("\n🚀 PIPELINE STARTED")

        job_id = req.job_id or str(uuid.uuid4())
        user_id = req.user_id

        # =========================
        # STEP 1: RUN PIPELINE
        # =========================
        result = pipeline_service.run(
            prompt=req.prompt,
            user_id=user_id,
            job_id=job_id
        )

        print("✅ Pipeline completed")

        # =========================
        # STEP 2: SAVE TEMP RESULT
        # =========================
        temp_blob = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{user_id}/{job_id}/temp_result.json"
        )

        temp_blob.upload_blob(
            json.dumps(result, indent=2),
            overwrite=True
        )

        print("💾 Temp result saved")

        # =========================
        # STEP 3: PREPARE SAFE DATA
        # =========================
        data_model = result.get("data_model") or {}
        final_dataset = result.get("final_dataset") or {}

        # ---- job_name ----
        fact_table = data_model.get("fact_table")
        job_name = f"{fact_table}_job" if fact_table else f"job_{job_id[:6]}"

        # ---- download_url ----
        dataset_name = final_dataset.get("dataset_name")
        download_url = None
        if dataset_name:
            download_url = f"/download/{user_id}/{job_id}/{dataset_name}"

        # =========================
        # STEP 4: SAVE ETL RESULT TO THREAD 🔥
        # =========================
        try:
            print("🧠 Saving ETL result to thread...")

            thread_service.add_action(user_id, job_id, {
                "type": "etl",
                "job_id": job_id,
                "job_name": job_name,
                "prompt": req.prompt,
                "download_url": download_url,
                "response": result
            })

            print("✅ ETL saved in thread")

        except Exception as e:
            print("⚠️ Thread save failed:", str(e))

        # =========================
        # STEP 5: RETURN RESPONSE
        # =========================
        return {
            "status": "success",
            "job_id": job_id,
            "job_name": job_name,   # 🔥 added

            "data_model": data_model,
            "relationships": result.get("relationships"),
            "schemas": result.get("schemas"),
            "final_dataset": final_dataset,

            "download_url": download_url
        }

    except Exception as e:
        print("❌ PIPELINE ERROR:", str(e))
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
from fastapi import HTTPException
from datetime import datetime
import json
import requests

@app.post("/save-job")
def save_job(req: SaveJobRequest):
    try:
        print("\n💾 SAVE JOB STARTED")

        user_id = req.user_id
        job_id = req.job_id

        # =========================
        # STEP 1: GET TEMP RESULT
        # =========================
        temp_blob = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{user_id}/{job_id}/temp_result.json"
        )

        if not temp_blob.exists():
            raise HTTPException(status_code=404, detail="Temp result not found. Run pipeline first.")

        result = json.loads(temp_blob.download_blob().readall())

        print("✅ Temp result loaded")

        # =========================
        # STEP 2: SAVE FINAL JOB JSON
        # =========================
        job_blob = pipeline_service.blob_service.get_blob_client(
            container="jobs",
            blob=f"{user_id}/{job_id}/job.json"
        )

        job_data = {
            "user_id": user_id,
            "job_id": job_id,
            "created_at": datetime.utcnow().isoformat(),
            "result": result
        }

        job_blob.upload_blob(json.dumps(job_data, indent=2), overwrite=True)

        print("✅ Job saved to jobs container")

        # =========================
        # STEP 3: REGISTER DATASET WITH AUTOML
        # =========================
        upload_result = {}

        try:
            print("📡 Registering dataset with AutoML...")

            upload_url = "https://api.veriton.ai/api/service3/upload_file_V"

            onelake_path = result.get("final_dataset", {}).get("onelake_path")

            print("📁 OneLake Path:", onelake_path)

            if not onelake_path:
                raise Exception("onelake_path missing in result")

            response = requests.post(
                upload_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "file_path": onelake_path,
                    "session_id": req.session_id,
                    "user_email": req.user_email,
                    "query": ""
                }
            )

            print("🔍 Status Code:", response.status_code)
            print("🔍 Raw Response:", response.text)

            # SAFE JSON PARSE
            try:
                upload_result = response.json()
            except Exception:
                upload_result = {
                    "error": "Invalid JSON response",
                    "raw_response": response.text
                }

            if response.status_code != 200:
                print("⚠️ AutoML upload failed")

            print("✅ AutoML upload processed")

        except Exception as e:
            print("⚠️ AutoML upload exception:", str(e))
            upload_result = {"error": str(e)}

        # =========================
        # STEP 4: SAVE IN THREAD 🔥
        # =========================
        try:
            print("🧠 Saving upload result to thread...")

            thread_service.add_action(user_id, job_id, {
                "type": "file_upload",
                "response": upload_result
            })

            print("✅ Thread updated")

        except Exception as e:
            print("⚠️ Thread save failed:", str(e))

        # =========================
        # STEP 5: RETURN RESPONSE
        # =========================
        return {
            "status": "success",
            "job_id": job_id,
            "upload_result": upload_result
        }

    except Exception as e:
        print("❌ SAVE JOB ERROR:", str(e))
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
    
#####power-bi
@app.post("/generate_powerbi_dashboard")
def generate_dashboard(req: PowerBIDashboardRequest):

    try:
        print("\n📊 POWERBI GENERATION STARTED")

        # =========================
        # STEP 1: GENERATE DASHBOARD
        # =========================
        result = generate_powerbi_dashboard(req.csv_blob, req.user_prompt)

        clean_result = sanitize_for_json(result)

        print("✅ PowerBI generated")

        # =========================
        # STEP 2: SAVE TO THREAD 🔥
        # =========================
        try:
            print("🧠 Saving PowerBI result to thread...")

            thread_service.add_action(req.user_id, req.job_id, {
                "type": "powerbi",
                "prompt": req.user_prompt,
                "response": clean_result
            })

            print("✅ PowerBI saved in thread")

        except Exception as e:
            print("⚠️ Thread save failed:", str(e))

        # =========================
        # STEP 3: RETURN RESPONSE
        # =========================
        return JSONResponse(content=clean_result)

    except Exception as e:
        print("❌ POWERBI ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
####automl-run    
@app.post("/automl/run")
def run_automl(req: AutoMLRequest):
    try:
        print("🚀 Starting AutoML...")

        start_url = "https://api.veriton.ai/api/service3/process_task_query_v"

        # =========================
        # STEP 1: START JOB
        # =========================
        start_res = requests.post(
            start_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "session_id": req.session_id,
                "query": req.query,
                "user_email": req.user_email
            }
        ).json()

        if start_res.get("status") != "started":
            raise Exception("Failed to start AutoML")

        job_id_ext = start_res.get("job_id")

        print("🧠 Job started:", job_id_ext)

        # =========================
        # STEP 2: POLL STATUS API
        # =========================
        status_url = f"https://api.veriton.ai/api/service3/process-task-query-status/{job_id_ext}"

        import time

        while True:
            time.sleep(start_res.get("poll_every_seconds", 10))

            poll_res = requests.get(
                status_url,
                params={"user_email": req.user_email}
            ).json()

            status = poll_res.get("status")

            print("⏳ Status:", status)

            if status == "success":
                print("✅ AutoML completed")

                # 🔥 SAVE IN THREAD
                thread_service.add_action(req.user_id, req.job_id, {
                    "type": "automl",
                    "prompt": req.query,
                    "response": poll_res
                })

                return poll_res

            elif status == "failed":
                raise Exception("AutoML job failed")

    except Exception as e:
        print("❌ AutoML error:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
from fastapi import HTTPException

####threads
@app.get("/threads")
def get_thread(user_id: str, job_id: str):
    try:
        thread = thread_service.get_thread(user_id, job_id)

        return {
            "status": "success",
            "thread_id": thread.get("thread_id"),
            "user_id": thread.get("user_id"),
            "job_id": thread.get("job_id"),
            "created_at": thread.get("created_at"),
            "job_summary": thread.get("job_summary", {}),
            "messages": thread.get("messages", []),
            "actions": thread.get("actions", [])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
#get user jobs
# =========================
def get_user_jobs(user_id):

    container = pipeline_service.blob_service.get_container_client("jobs")

    jobs = []
    seen = set()

    for blob in container.list_blobs(name_starts_with=f"{user_id}/"):

        if blob.name.endswith(".json") and "temp_result" not in blob.name:

            blob_client = container.get_blob_client(blob.name)
            data = json.loads(blob_client.download_blob().readall())

            job_id = data.get("job_id")

            if job_id in seen:
                continue

            seen.add(job_id)

            jobs.append({
                "job_id": job_id,
                "job_name": data.get("job_name", f"Job {job_id[:6]}")
            })

    return jobs

# =========================
# RUN PIPELINE
# =========================
def run_pipeline_execution(user_id, pipeline_id):

    print(f"\n🚀 Running pipeline: {pipeline_id}")

    pipeline = pipeline_manager.get_pipeline(user_id, pipeline_id)

    for job_id in pipeline["jobs"]:
        try:
            result = pipeline_service.run(
                prompt="pipeline run",
                user_id=user_id,
                job_id=job_id
            )

            thread_service.add_action(user_id, job_id, {
                "type": "etl",
                "prompt": "pipeline run",
                "response": result
            })

        except Exception as e:
            print("❌ Job failed:", job_id, str(e))

# =========================
# CHAT
# =========================
@app.post("/chat")
def chat(req: ChatRequest):

    user_id = req.user_id
    message = req.message.lower().strip()

    # ✅ SAFE STATE
    state = state_store.get(user_id, {})

    print("STATE:", state)
    print("MESSAGE:", message)

    # =========================
    # 🔥 GLOBAL FAST PIPELINE CREATION (WORKS FROM ANY STATE)
    # =========================
    parsed = parse_pipeline_prompt(message)

    if parsed and req.selected_jobs:

        pipeline = pipeline_manager.create_pipeline(
            user_id,
            parsed["pipeline_name"],
            req.selected_jobs
        )

        schedule = parsed["schedule"]

        if schedule:
            pipeline_manager.update_schedule(
                user_id,
                pipeline["pipeline_id"],
                schedule
            )

            schedule_service.save_schedule({
                "user_id": user_id,
                "pipeline_id": pipeline["pipeline_id"],
                "schedule": schedule
            })

            schedule_pipeline(
                run_pipeline_execution,
                user_id,
                pipeline["pipeline_id"],
                schedule
            )

            pipeline = pipeline_manager.get_pipeline(
                user_id,
                pipeline["pipeline_id"]
            )

            return {
                "response": "✅ Pipeline created and scheduled",
                "pipeline": pipeline
            }

        return {
            "response": "✅ Pipeline created",
            "pipeline": pipeline
        }

    # =========================
    # ⚠️ FAST MODE BUT NO JOBS
    # =========================
    if parsed and not req.selected_jobs:
        return {"response": "Please select jobs before creating pipeline"}

    # =========================
    # START PIPELINE FLOW
    # =========================
    if "pipeline" in message and "create" in message:

        jobs = get_user_jobs(user_id)

        state_store[user_id] = {
            "stage": "select_jobs",
            "jobs": jobs
        }

        return {
            "response": "Select jobs for pipeline",
            "jobs": jobs
        }

    # =========================
    # SELECT JOBS
    # =========================
    if state.get("stage") == "select_jobs":

        if not req.selected_jobs:
            return {"response": "Please select at least one job"}

        state["selected_jobs"] = req.selected_jobs
        state["stage"] = "name_pipeline"

        return {
            "response": "Enter pipeline name OR use quick format:\nExample: sales pipeline daily 10:00"
        }

    # =========================
    # PIPELINE NAME
    # =========================
    if state.get("stage") == "name_pipeline":

        state["pipeline_name"] = message
        state["stage"] = "ask_schedule"

        return {"response": "Do you want to schedule it? (yes/no)"}

    # =========================
    # ASK SCHEDULE
    # =========================
    if state.get("stage") == "ask_schedule":

        if "yes" in message:
            state["stage"] = "schedule_type"
            return {"response": "daily / weekly / monthly?"}
        else:
            pipeline = pipeline_manager.create_pipeline(
                user_id,
                state["pipeline_name"],
                state["selected_jobs"]
            )

            state_store[user_id] = {}

            return {
                "response": "✅ Pipeline created",
                "pipeline": pipeline
            }

    # =========================
    # SCHEDULE TYPE
    # =========================
    if state.get("stage") == "schedule_type":

        if message not in ["daily", "weekly", "monthly"]:
            return {"response": "Please choose: daily / weekly / monthly"}

        state["schedule"] = {"type": message}

        if message == "weekly":
            state["stage"] = "schedule_day"
            return {"response": "Enter day (0=Mon ... 6=Sun)"}

        if message == "monthly":
            state["stage"] = "schedule_day"
            return {"response": "Enter date (1-31)"}

        state["stage"] = "schedule_time"
        return {"response": "Enter time (HH:MM)"}

    # =========================
    # DAY INPUT
    # =========================
    if state.get("stage") == "schedule_day":

        state["schedule"]["day"] = message
        state["stage"] = "schedule_time"

        return {"response": "Enter time (HH:MM)"}

    # =========================
    # TIME INPUT
    # =========================
    if state.get("stage") == "schedule_time":

        import re

        if not re.match(r"^\d{1,2}:\d{2}$", message):
            return {"response": "Invalid time format. Use HH:MM"}

        hour, minute = map(int, message.split(":"))

        schedule = state["schedule"]
        schedule["hour"] = hour
        schedule["minute"] = minute

        # CREATE PIPELINE
        pipeline = pipeline_manager.create_pipeline(
            user_id,
            state["pipeline_name"],
            state["selected_jobs"]
        )

        # SAVE SCHEDULE
        pipeline_manager.update_schedule(
            user_id,
            pipeline["pipeline_id"],
            schedule
        )

        schedule_service.save_schedule({
            "user_id": user_id,
            "pipeline_id": pipeline["pipeline_id"],
            "schedule": schedule
        })

        schedule_pipeline(
            run_pipeline_execution,
            user_id,
            pipeline["pipeline_id"],
            schedule
        )

        state_store[user_id] = {}

        pipeline = pipeline_manager.get_pipeline(
            user_id,
            pipeline["pipeline_id"]
        )

        return {
            "response": "✅ Pipeline created and scheduled",
            "pipeline": pipeline
        }

    # =========================
    # DEFAULT
    # =========================
    return {"response": "I didn't understand"}
    
# =========================
# START UP
# =========================
@app.on_event("startup")
def load_schedules():

    print("🔄 Loading schedules from storage...")

    schedules = schedule_service.get_all_schedules()

    for s in schedules:
        try:
            schedule_pipeline(
                run_pipeline_execution,
                s["user_id"],
                s["pipeline_id"],
                s["schedule"]
            )
        except Exception as e:
            print("⚠️ Failed to restore schedule:", str(e))

# =========================
# LIST PIPELINES
# =========================
@app.get("/pipelines")
def get_pipelines(user_id: str):
    return pipeline_manager.list_pipelines(user_id)

# =========================
# ADD JOB TO PIPELINE
# =========================
@app.post("/pipelines/add-job")
def add_job_to_pipeline(user_id: str, pipeline_id: str, job_id: str):

    pipeline = pipeline_manager.get_pipeline(user_id, pipeline_id)

    if job_id not in pipeline["jobs"]:
        pipeline["jobs"].append(job_id)

    pipeline_manager.update_schedule(user_id, pipeline_id, pipeline.get("schedule"))

    return pipeline

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "running"}
