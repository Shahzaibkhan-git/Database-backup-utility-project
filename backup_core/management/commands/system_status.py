from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from backup_core.models import BackupArtifact, BackupJob, RestoreJob, Schedule


class Command(BaseCommand):
    help = "Show one-shot status for backup system health."

    def handle(self, *args, **options):
        now = timezone.now()
        metadata_db = Path(settings.DATABASES["default"]["NAME"]).resolve()
        target_db = Path(getattr(settings, "TARGET_SQLITE_DB_PATH", metadata_db)).resolve()
        backup_root = Path(getattr(settings, "BACKUP_ROOT", settings.BASE_DIR / "backups")).resolve()
        log_file = Path(getattr(settings, "BACKUP_LOG_FILE", settings.BASE_DIR / "logs" / "backup.log")).resolve()

        total_jobs = BackupJob.objects.count()
        success_jobs = BackupJob.objects.filter(status=BackupJob.STATUS_SUCCESS).count()
        failed_jobs = BackupJob.objects.filter(status=BackupJob.STATUS_FAILED).count()
        total_artifacts = BackupArtifact.objects.count()
        total_restores = RestoreJob.objects.count()
        failed_restores = RestoreJob.objects.filter(status=RestoreJob.STATUS_FAILED).count()
        active_schedules = Schedule.objects.filter(is_active=True).count()
        due_schedules = Schedule.objects.filter(is_active=True).filter(
            Q(next_run_at__isnull=True) | Q(next_run_at__lte=now)
        ).filter(
            Q(lease_expires_at__isnull=True) | Q(lease_expires_at__lte=now)
        ).count()
        leased_schedules = Schedule.objects.filter(is_active=True, lease_expires_at__gt=now).count()

        latest_artifact = BackupArtifact.objects.select_related("backup_job").first()
        latest_restore = RestoreJob.objects.select_related("backup_artifact").first()
        next_schedule = Schedule.objects.filter(is_active=True).exclude(next_run_at__isnull=True).order_by("next_run_at").first()

        broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
        backend_url = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

        self.stdout.write("Backup Utility System Status")
        self.stdout.write(f"timestamp={now.isoformat()}")
        self.stdout.write(f"metadata_db={metadata_db} exists={metadata_db.exists()}")
        self.stdout.write(f"target_sqlite_db={target_db} exists={target_db.exists()}")
        self.stdout.write(f"backup_root={backup_root} exists={backup_root.exists()}")
        self.stdout.write(f"log_file={log_file} exists={log_file.exists()}")
        self.stdout.write(f"celery_broker={self._safe_url(broker_url)}")
        self.stdout.write(f"celery_backend={self._safe_url(backend_url)}")
        self.stdout.write(
            f"backup_jobs total={total_jobs} success={success_jobs} failed={failed_jobs} artifacts={total_artifacts}"
        )
        self.stdout.write(f"restore_jobs total={total_restores} failed={failed_restores}")
        self.stdout.write(f"schedules active={active_schedules} due_now={due_schedules} leased={leased_schedules}")

        if next_schedule:
            self.stdout.write(
                "next_schedule="
                f"id={next_schedule.id} backup_job_id={next_schedule.backup_job_id} "
                f"run_at={next_schedule.next_run_at.isoformat()}"
            )
        else:
            self.stdout.write("next_schedule=none")

        if latest_artifact:
            self.stdout.write(
                "latest_artifact="
                f"id={latest_artifact.id} job={latest_artifact.backup_job.name} "
                f"created={latest_artifact.created_at.isoformat()} path={latest_artifact.file_path}"
            )
        else:
            self.stdout.write("latest_artifact=none")

        if latest_restore:
            artifact_id = latest_restore.backup_artifact_id if latest_restore.backup_artifact_id else "-"
            self.stdout.write(
                "latest_restore="
                f"id={latest_restore.id} status={latest_restore.status} "
                f"artifact_id={artifact_id} created={latest_restore.created_at.isoformat()}"
            )
        else:
            self.stdout.write("latest_restore=none")

    def _safe_url(self, value: str) -> str:
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.netloc:
            return value

        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        path = parsed.path or ""
        return f"{parsed.scheme}://{host}{port}{path}"
