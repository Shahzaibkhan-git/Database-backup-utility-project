from __future__ import annotations

import tempfile
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from django.utils import timezone

from backup_core.base import get_adapter
from backup_core.compression import decompress_file
from backup_core.encryption import decrypt_file
from backup_core.logger import get_logger
from backup_core.models import BackupArtifact, RestoreJob
from backup_core.notifications import send_slack_notification


class Command(BaseCommand):
    help = "Restore a database from a backup file."

    def add_arguments(self, parser):
        parser.add_argument("--artifact-id", type=int, help="BackupArtifact id from local metadata DB")
        parser.add_argument("--backup-file", help="Path to backup file (local)")

        parser.add_argument("--db-type", default="sqlite", choices=["sqlite", "postgres", "mysql", "mongo"])
        parser.add_argument("--db-path", help="SQLite destination database file path")
        parser.add_argument("--host")
        parser.add_argument("--port", type=int)
        parser.add_argument("--username")
        parser.add_argument("--password")
        parser.add_argument("--database")
        parser.add_argument("--uri")

        parser.add_argument("--tables", help="Comma-separated tables/collections for selective restore")
        parser.add_argument("--decrypt-key", help="Secret key if file is encrypted")
        parser.add_argument("--slack-webhook-url", help="Slack webhook URL")

    def handle(self, *args, **options):
        logger = get_logger("backup_core.restore")

        artifact, backup_file = self._resolve_backup_source(options)
        selected_tables = self._parse_tables(options.get("tables"))
        connection_params = self._build_connection_params(options)
        if options["db_type"] == "sqlite":
            connection_params["allow_create"] = True

        restoring_metadata_db = self._is_restoring_metadata_db(options, connection_params)
        if restoring_metadata_db:
            self.stdout.write(
                self.style.WARNING(
                    "Restoring over Django metadata database. "
                    "RestoreJob persistence will be skipped for this run."
                )
            )

        started_at = timezone.now()
        restore_job = None
        if not restoring_metadata_db:
            restore_job = RestoreJob.objects.create(
                backup_job=artifact.backup_job if artifact else None,
                backup_artifact=artifact,
                target_params=self._redact(connection_params),
                selected_tables=selected_tables or [],
                status=RestoreJob.STATUS_RUNNING,
                started_at=started_at,
            )

        try:
            adapter = get_adapter(options["db_type"], connection_params)
            adapter.test_connection()

            backup_path = Path(backup_file)
            if not backup_path.exists():
                raise CommandError(f"Backup file does not exist: {backup_path}")

            with tempfile.TemporaryDirectory(prefix="restore_work_") as tmp_dir:
                working_path = backup_path

                # Handle chained extensions like .gz.enc and single extensions.
                if working_path.suffix == ".enc":
                    decrypt_key = options.get("decrypt_key")
                    if not decrypt_key:
                        raise CommandError("Backup file is encrypted. Provide --decrypt-key.")
                    target_path = Path(tmp_dir) / working_path.with_suffix("").name
                    working_path = Path(decrypt_file(str(working_path), decrypt_key, output_path=str(target_path)))

                if working_path.suffix == ".gz":
                    target_path = Path(tmp_dir) / working_path.with_suffix("").name
                    working_path = Path(decompress_file(str(working_path), output_path=str(target_path)))

                if restoring_metadata_db:
                    connections.close_all()
                adapter.restore(str(working_path), tables=selected_tables)

            finished_at = timezone.now()
            duration = (finished_at - started_at).total_seconds()

            if restore_job is not None:
                restore_job.status = RestoreJob.STATUS_SUCCESS
                restore_job.finished_at = finished_at
                restore_job.duration_seconds = duration
                restore_job.error_message = ""
                restore_job.save(update_fields=["status", "finished_at", "duration_seconds", "error_message"])

            msg = "Restore completed successfully."
            if restore_job is not None:
                msg = f"{msg} restore_job_id={restore_job.id}"
            logger.info(msg)
            self.stdout.write(self.style.SUCCESS(msg))
            send_slack_notification(options.get("slack_webhook_url"), msg)

        except Exception as exc:
            finished_at = timezone.now()
            duration = (finished_at - started_at).total_seconds()

            if restore_job is not None:
                restore_job.status = RestoreJob.STATUS_FAILED
                restore_job.finished_at = finished_at
                restore_job.duration_seconds = duration
                restore_job.error_message = str(exc)
                try:
                    restore_job.save(update_fields=["status", "finished_at", "duration_seconds", "error_message"])
                except DatabaseError:
                    logger.exception("Failed to persist RestoreJob failure state.")

            logger.exception("Restore failed: %s", exc)
            send_slack_notification(options.get("slack_webhook_url"), f"Restore failed: {exc}")
            raise CommandError(f"Restore failed: {exc}") from exc

    def _resolve_backup_source(self, options: dict):
        artifact = None
        backup_file = options.get("backup_file")

        artifact_id = options.get("artifact_id")
        if artifact_id:
            try:
                artifact = BackupArtifact.objects.select_related("backup_job").get(id=artifact_id)
            except BackupArtifact.DoesNotExist as exc:
                raise CommandError(f"BackupArtifact with id={artifact_id} not found.") from exc

            if not backup_file:
                if artifact.storage_type != "local":
                    raise CommandError(
                        "Artifact is not local. Download it first and pass --backup-file explicitly."
                    )
                backup_file = artifact.file_path

        if not backup_file:
            raise CommandError("Provide --backup-file or --artifact-id.")

        return artifact, backup_file

    def _parse_tables(self, raw_value: str | None):
        if not raw_value:
            return None
        items = [item.strip() for item in raw_value.split(",") if item.strip()]
        return items or None

    def _build_connection_params(self, options: dict) -> dict:
        params = {}
        for key in ["db_path", "host", "port", "username", "password", "database", "uri"]:
            value = options.get(key)
            if value not in (None, ""):
                if key == "db_path":
                    params["path"] = value
                else:
                    params[key] = value
        return params

    def _redact(self, params: dict) -> dict:
        result = dict(params)
        for key in ["password", "uri", "token", "secret"]:
            if key in result and result[key]:
                result[key] = "***"
        return result

    def _is_restoring_metadata_db(self, options: dict, connection_params: dict) -> bool:
        if options.get("db_type") != "sqlite":
            return False

        metadata_db_path = Path(settings.DATABASES["default"]["NAME"]).resolve()
        default_target_path = Path(
            getattr(settings, "TARGET_SQLITE_DB_PATH", settings.DATABASES["default"]["NAME"])
        ).resolve()
        target_db_path = Path(connection_params.get("path") or default_target_path).resolve()
        return target_db_path == metadata_db_path
