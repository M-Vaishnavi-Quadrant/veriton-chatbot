from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.start()


def schedule_pipeline(run_fn, user_id, pipeline_id, schedule):

    if schedule["type"] == "daily":
        scheduler.add_job(
            run_fn,
            "cron",
            hour=schedule["hour"],
            minute=schedule["minute"],
            args=[user_id, pipeline_id]
        )

    elif schedule["type"] == "weekly":
        scheduler.add_job(
            run_fn,
            "cron",
            day_of_week=schedule["day"],
            hour=schedule["hour"],
            minute=schedule["minute"],
            args=[user_id, pipeline_id]
        )

    elif schedule["type"] == "monthly":
        scheduler.add_job(
            run_fn,
            "cron",
            day=schedule["day"],
            hour=schedule["hour"],
            minute=schedule["minute"],
            args=[user_id, pipeline_id]
        )
