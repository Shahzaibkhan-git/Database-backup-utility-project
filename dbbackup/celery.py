from __future__ import annotations

import os

from celery import Celery

# Ensure Celery uses Django settings.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dbbackup.settings")

app = Celery("dbbackup")

# Load Celery settings from Django settings using CELERY_ prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Reasonable defaults for local development (Redis broker/backend).
app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", app.conf.broker_url or "redis://localhost:6379/0")
app.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND",
    app.conf.result_backend or "redis://localhost:6379/1",
)
app.conf.task_serializer = os.environ.get("CELERY_TASK_SERIALIZER", "json")
app.conf.result_serializer = os.environ.get("CELERY_RESULT_SERIALIZER", "json")
app.conf.accept_content = ["json"]
app.conf.timezone = os.environ.get("CELERY_TIMEZONE", "UTC")
app.conf.enable_utc = True

app.autodiscover_tasks()

# Run the internal backup scheduler loop every N seconds via Celery Beat.
# This triggers one pass of backup_core Schedule records each time.
beat_interval = int(os.environ.get("BACKUP_SCHEDULER_BEAT_INTERVAL_SECONDS", "60"))
if beat_interval > 0:
    app.conf.beat_schedule = {
        **(app.conf.beat_schedule or {}),
        "backup-core-run-scheduler-once": {
            "task": "backup_core.tasks.run_scheduler_once",
            "schedule": beat_interval,
            "kwargs": {"max_jobs": 20, "dry_run": False},
        },
    }


@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
