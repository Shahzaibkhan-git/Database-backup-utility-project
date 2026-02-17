from __future__ import annotations

import io
import os
import time

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from backup_core.logger import get_logger
from backup_core.models import Schedule
from backup_core.scheduler import (
    claim_schedule,
    get_due_schedules,
    get_next_run_at,
    mark_schedule_failed,
    mark_schedule_ran,
)


class Command(BaseCommand):
    help = "Run scheduled backup jobs from Schedule records."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true", help="Run one scheduler pass and exit")
        parser.add_argument(
            "--interval-seconds",
            type=int,
            default=60,
            help="Polling interval for continuous mode (default: 60)",
        )
        parser.add_argument("--max-jobs", type=int, default=20, help="Maximum schedules to process per pass")
        parser.add_argument("--schedule-id", type=int, help="Run only one schedule ID")
        parser.add_argument("--dry-run", action="store_true", help="Show what would run without executing backups")
        parser.add_argument("--quiet", action="store_true", help="Suppress command stdout output")
        parser.add_argument(
            "--lease-seconds",
            type=int,
            default=300,
            help="Lease duration to prevent duplicate schedule execution (default: 300)",
        )

    def handle(self, *args, **options):
        logger = get_logger("backup_core.scheduler")
        once = options["once"]
        dry_run = options["dry_run"]
        quiet = options["quiet"]
        interval = max(options["interval_seconds"], 1)
        max_jobs = max(options["max_jobs"], 1)
        schedule_id = options.get("schedule_id")
        lease_seconds = max(int(options["lease_seconds"]), 1)
        command_stdout = io.StringIO() if quiet else self.stdout
        command_stderr = io.StringIO() if quiet else self.stderr

        if not quiet:
            self.stdout.write(self.style.SUCCESS("Scheduler started."))

        while True:
            processed = self._run_pass(
                logger=logger,
                dry_run=dry_run,
                max_jobs=max_jobs,
                schedule_id=schedule_id,
                lease_seconds=lease_seconds,
                command_stdout=command_stdout,
                command_stderr=command_stderr,
            )

            if once:
                if not quiet:
                    self.stdout.write(self.style.SUCCESS(f"Scheduler finished. processed={processed}"))
                break

            time.sleep(interval)

    def _run_pass(
        self,
        logger,
        dry_run: bool,
        max_jobs: int,
        schedule_id: int | None,
        lease_seconds: int,
        command_stdout,
        command_stderr,
    ) -> int:
        now = timezone.now()
        schedules = get_due_schedules(now)

        if schedule_id is not None:
            schedules = schedules.filter(id=schedule_id)

        schedule_ids = list(schedules.values_list("id", flat=True)[:max_jobs])

        if not schedule_ids:
            logger.info("No due schedules at %s", now.isoformat())
            return 0

        processed = 0
        for schedule_id_value in schedule_ids:
            schedule = claim_schedule(schedule_id_value, lease_seconds=lease_seconds, now=now)
            if schedule is None:
                continue

            processed += 1
            try:
                self._run_schedule(
                    schedule,
                    logger=logger,
                    dry_run=dry_run,
                    now=now,
                    command_stdout=command_stdout,
                    command_stderr=command_stderr,
                )
            except Exception as exc:  # pragma: no cover
                try:
                    next_run_at = get_next_run_at(schedule.cron_expression, after=now)
                except ValueError:
                    schedule.is_active = False
                    schedule.last_error = str(exc)[:4000]
                    schedule.lease_expires_at = None
                    schedule.save(update_fields=["is_active", "last_error", "lease_expires_at"])
                    logger.exception(
                        "Schedule %s disabled because cron is invalid and cannot compute next run: %s",
                        schedule.id,
                        exc,
                    )
                    continue

                failure = mark_schedule_failed(schedule, str(exc), next_run_at=next_run_at, now=now)
                if failure["state"] == "retrying":
                    logger.exception(
                        "Schedule %s failed (attempt %s/%s). Retry in %ss at %s: %s",
                        schedule.id,
                        failure["attempt"],
                        failure["max_retries"],
                        failure["delay_seconds"],
                        failure["next_run_at"].isoformat() if failure["next_run_at"] else "-",
                        exc,
                    )
                else:
                    logger.exception(
                        "Schedule %s failed after max retries. Next cron run at %s: %s",
                        schedule.id,
                        failure["next_run_at"].isoformat() if failure["next_run_at"] else "-",
                        exc,
                    )

        return processed

    def _run_schedule(self, schedule: Schedule, logger, dry_run: bool, now, command_stdout, command_stderr):
        template = schedule.backup_job

        try:
            next_run = get_next_run_at(schedule.cron_expression, after=now)
        except ValueError as exc:
            raise ValueError(f"Invalid cron for schedule {schedule.id} ({schedule.cron_expression}): {exc}") from exc

        if dry_run:
            logger.info(
                "[DRY RUN] schedule_id=%s backup_job_id=%s cron='%s' next_run=%s",
                schedule.id,
                template.id,
                schedule.cron_expression,
                next_run.isoformat(),
            )
            schedule.lease_expires_at = None
            schedule.save(update_fields=["lease_expires_at"])
            return

        backup_options = self._build_backup_options(template)
        logger.info(
            "Running schedule_id=%s backup_job_id=%s db_type=%s storage=%s",
            schedule.id,
            template.id,
            template.db_type,
            template.storage_type,
        )

        call_command(
            "backup_db",
            stdout=command_stdout,
            stderr=command_stderr,
            **backup_options,
        )
        mark_schedule_ran(schedule, next_run_at=next_run)

    def _build_backup_options(self, template):
        params = dict(template.connection_params or {})
        self._ensure_non_redacted(params)
        job_name = template.name
        if not job_name.endswith("-scheduled"):
            job_name = f"{job_name}-scheduled"

        options = {
            "name": job_name,
            "db_type": template.db_type,
            "backup_type": template.backup_type,
            "storage": template.storage_type,
            "output_dir": template.destination or str(getattr(settings, "BACKUP_ROOT", settings.BASE_DIR / "backups")),
            "compress": template.is_compressed,
        }

        self._merge_db_connection_params(options, params)
        self._merge_storage_options(options, params)

        tables = params.get("tables")
        if isinstance(tables, list) and tables:
            options["tables"] = ",".join(str(item).strip() for item in tables if str(item).strip())
        elif isinstance(tables, str) and tables.strip():
            options["tables"] = tables.strip()

        if template.is_encrypted:
            encrypt_key = os.environ.get("BACKUP_ENCRYPT_KEY")
            if not encrypt_key:
                raise ValueError(
                    "Template requires encryption but BACKUP_ENCRYPT_KEY env var is not set."
                )
            options["encrypt_key"] = encrypt_key

        return options

    def _merge_db_connection_params(self, options: dict, params: dict):
        mapping = {
            "path": "db_path",
            "host": "host",
            "port": "port",
            "username": "username",
            "password": "password",
            "database": "database",
            "uri": "uri",
        }
        for source_key, target_key in mapping.items():
            value = params.get(source_key)
            if value not in (None, ""):
                options[target_key] = value

    def _merge_storage_options(self, options: dict, params: dict):
        mapping = {
            "bucket": "bucket",
            "container": "container",
            "prefix": "prefix",
            "region": "region",
            "azure_connection_string": "azure_connection_string",
            "slack_webhook_url": "slack_webhook_url",
            "filename": "filename",
        }
        for source_key, target_key in mapping.items():
            value = params.get(source_key)
            if value not in (None, ""):
                options[target_key] = value

    def _ensure_non_redacted(self, params: dict):
        redacted_fields = [
            key
            for key in ["password", "uri", "azure_connection_string", "token", "secret"]
            if str(params.get(key, "")).strip() == "***"
        ]
        if redacted_fields:
            joined = ", ".join(redacted_fields)
            raise ValueError(
                f"Schedule connection_params has redacted values for: {joined}. "
                "Store real values before running scheduler."
            )
