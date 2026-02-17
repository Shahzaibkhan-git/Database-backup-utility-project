from __future__ import annotations

import io

from django.core.management import call_command

from celery import shared_task

from backup_core.logger import get_logger


@shared_task(bind=True, name="backup_core.tasks.run_scheduler_once")
def run_scheduler_once(self, max_jobs: int = 20, schedule_id: int | None = None, dry_run: bool = False):
    """
    Execute one scheduler pass inside a Celery worker.
    This reuses the existing management command logic.
    """
    logger = get_logger("backup_core.celery")
    logger.info(
        "Celery task started: run_scheduler_once max_jobs=%s schedule_id=%s dry_run=%s",
        max_jobs,
        schedule_id,
        dry_run,
    )

    call_command(
        "run_scheduler",
        once=True,
        max_jobs=max_jobs,
        schedule_id=schedule_id,
        dry_run=dry_run,
        quiet=True,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    logger.info("Celery task finished: run_scheduler_once")
    return {"status": "ok", "max_jobs": max_jobs, "schedule_id": schedule_id, "dry_run": dry_run}
