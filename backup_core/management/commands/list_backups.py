from __future__ import annotations

from django.core.management.base import BaseCommand

from backup_core.models import BackupArtifact


class Command(BaseCommand):
    help = "List recent backup artifacts from metadata DB."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=20)

    def handle(self, *args, **options):
        artifacts = BackupArtifact.objects.select_related("backup_job")[: options["limit"]]

        if not artifacts:
            self.stdout.write("No backup artifacts found.")
            return

        for artifact in artifacts:
            self.stdout.write(
                f"id={artifact.id} job={artifact.backup_job.name} path={artifact.file_path} "
                f"size={artifact.size_bytes} created={artifact.created_at.isoformat()}"
            )
