from pydantic import BaseModel

class PipelineRequest(BaseModel):
    user_id: str
    job_id: str   # 👈 REQUIRED
    prompt: str
