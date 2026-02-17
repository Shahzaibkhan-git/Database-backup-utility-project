from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from backup_core.models import BackupJob, Schedule
from backup_core.scheduler import get_next_run_at


class Command(BaseCommand):
    help = "Create a schedule for an existing BackupJob."

    def add_arguments(self, parser):
        parser.add_argument("--backup-job-id", type=int, required=True, help="BackupJob id to schedule")
        parser.add_argument("--cron", required=True, help="Cron expression (5 fields) or alias like @hourly")
        parser.add_argument("--inactive", action="store_true", help="Create schedule as inactive")
        parser.add_argument(
            "--due-now",
            action="store_true",
            help="Set next_run_at to now so scheduler will pick it immediately",
        )

    def handle(self, *args, **options):
        backup_job_id = options["backup_job_id"]
        cron_expression = options["cron"]
        inactive = options["inactive"]
        due_now = options["due_now"]

        try:
            backup_job = BackupJob.objects.get(id=backup_job_id)
        except BackupJob.DoesNotExist as exc:
            raise CommandError(f"BackupJob with id={backup_job_id} not found.") from exc

        now = timezone.now()
        try:
            next_run_at = now if due_now else get_next_run_at(cron_expression, after=now)
        except ValueError as exc:
            raise CommandError(f"Invalid cron expression: {exc}") from exc

        schedule = Schedule.objects.create(
            backup_job=backup_job,
            cron_expression=cron_expression,
            is_active=not inactive,
            next_run_at=next_run_at,
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Schedule created. id={schedule.id} backup_job_id={backup_job.id} "
                f"active={schedule.is_active} next_run_at={schedule.next_run_at.isoformat()}"
            )
        )
