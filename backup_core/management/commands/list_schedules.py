from __future__ import annotations

from django.core.management.base import BaseCommand

from backup_core.models import Schedule


class Command(BaseCommand):
    help = "List schedules."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)
        parser.add_argument("--active-only", action="store_true")

    def handle(self, *args, **options):
        schedules = Schedule.objects.select_related("backup_job")
        if options["active_only"]:
            schedules = schedules.filter(is_active=True)

        schedules = schedules[: options["limit"]]

        if not schedules:
            self.stdout.write("No schedules found.")
            return

        for schedule in schedules:
            next_run = schedule.next_run_at.isoformat() if schedule.next_run_at else "-"
            last_run = schedule.last_run_at.isoformat() if schedule.last_run_at else "-"
            self.stdout.write(
                f"id={schedule.id} backup_job_id={schedule.backup_job_id} "
                f"name={schedule.backup_job.name} active={schedule.is_active} "
                f"cron='{schedule.cron_expression}' next_run={next_run} last_run={last_run}"
            )
