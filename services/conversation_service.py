import uuid
from app.services.session_store import session_store


class ConversationService:

    def __init__(self, pipeline_manager):
        self.pipeline_manager = pipeline_manager

    def start(self, metadata):

        session_id = str(uuid.uuid4())

        session_store.create(session_id, {
            "step": "ask_save",
            "metadata": metadata
        })

        return {
            "session_id": session_id,
            "message": "Do you want to save this pipeline?",
            "options": ["yes", "no"]
        }

    def handle(self, session_id, user_input):

        session = session_store.get(session_id)

        if not session:
            return {"error": "Invalid session"}

        step = session["step"]

        # STEP 1
        if step == "ask_save":

            if user_input.lower() != "yes":
                session_store.delete(session_id)
                return {"message": "Pipeline not saved"}

            session["step"] = "ask_name"
            session_store.update(session_id, session)

            return {"message": "Enter pipeline name"}

        # STEP 2
        elif step == "ask_name":

            session["pipeline_name"] = user_input
            session["step"] = "ask_schedule"
            session_store.update(session_id, session)

            return {
                "message": "Choose schedule",
                "options": ["daily", "weekly", "monthly"]
            }

        # STEP 3
        elif step == "ask_schedule":

            session["schedule_type"] = user_input
            session["step"] = "ask_time"
            session_store.update(session_id, session)

            return {"message": "Enter time (HH:MM)"}

        # STEP 4 (SAVE)
        elif step == "ask_time":

            metadata = session["metadata"]

            schedule = {
                "type": session["schedule_type"],
                "time": user_input
            }

            print("🔥 CREATE PIPELINE CALLED")

            self.pipeline_manager.create_pipeline(
                pipeline_name=session["pipeline_name"],
                prompt=metadata["prompt"],      # ✅ FIX
                user_id=metadata["user_id"],    # ✅ FIX
                schedule=schedule
            )

            session_store.delete(session_id)

            return {
                "message": "✅ Pipeline saved successfully"
            }