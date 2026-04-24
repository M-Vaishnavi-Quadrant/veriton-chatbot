from apscheduler.schedulers.background import BackgroundScheduler

class SchedulerService:

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def schedule_pipeline(self, pipeline_name, user_id, schedule, runner):

        def job():
            print(f"[SCHEDULER] Running: {pipeline_name}")
            runner(pipeline_name, user_id)

        hour, minute = map(int, schedule["time"].split(":"))

        if schedule["type"] == "daily":
            self.scheduler.add_job(job, "cron", hour=hour, minute=minute)

        elif schedule["type"] == "weekly":
            self.scheduler.add_job(job, "cron", day_of_week="mon", hour=hour, minute=minute)

        elif schedule["type"] == "monthly":
            self.scheduler.add_job(job, "cron", day=1, hour=hour, minute=minute)