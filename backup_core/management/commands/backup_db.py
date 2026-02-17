from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from backup_core.azure import AzureBlobStorageBackend
from backup_core.base import AdapterError, get_adapter
from backup_core.compression import compress_file
from backup_core.encryption import encrypt_file
from backup_core.gcs import GCSStorageBackend
from backup_core.local import LocalStorageBackend
from backup_core.logger import get_logger
from backup_core.models import BackupArtifact, BackupJob
from backup_core.notifications import send_slack_notification
from backup_core.s3 import S3StorageBackend


class Command(BaseCommand):
    help = "Backup a database from CLI."

    def add_arguments(self, parser):
        parser.add_argument("--name", default="manual-backup")
        parser.add_argument("--db-type", default="sqlite", choices=["sqlite", "postgres", "mysql", "mongo"])
        parser.add_argument("--db-path", help="SQLite database file path")
        parser.add_argument("--host")
        parser.add_argument("--port", type=int)
        parser.add_argument("--username")
        parser.add_argument("--password")
        parser.add_argument("--database")
        parser.add_argument("--uri")

        parser.add_argument("--backup-type", default="full", choices=["full", "incremental", "differential"])
        parser.add_argument("--tables", help="Comma-separated tables/collections for selective backup")

        parser.add_argument("--output-dir", default=str(getattr(settings, "BACKUP_ROOT", settings.BASE_DIR / "backups")))
        parser.add_argument("--filename")
        parser.add_argument("--compress", action="store_true")
        parser.add_argument("--encrypt-key", help="Secret key to encrypt backup file")

        parser.add_argument("--storage", default="local", choices=["local", "s3", "gcs", "azure"])
        parser.add_argument("--bucket", help="Bucket name for S3/GCS")
        parser.add_argument("--container", help="Container name for Azure")
        parser.add_argument("--prefix", default="")
        parser.add_argument("--region", help="AWS region for S3 uploads")
        parser.add_argument("--azure-connection-string", help="Azure Blob connection string")

        parser.add_argument("--slack-webhook-url", help="Slack webhook URL")

    def handle(self, *args, **options):
        logger = get_logger("backup_core.backup")
        started_at = timezone.now()

        tables = self._parse_tables(options.get("tables"))
        connection_params = self._build_connection_params(options)

        job = BackupJob.objects.create(
            name=options["name"],
            db_type=options["db_type"],
            backup_type=options["backup_type"],
            connection_params=self._redact(connection_params),
            storage_type=options["storage"],
            destination=options.get("output_dir", ""),
            status=BackupJob.STATUS_RUNNING,
            is_compressed=options["compress"],
            is_encrypted=bool(options.get("encrypt_key")),
            started_at=started_at,
        )

        try:
            adapter = get_adapter(options["db_type"], connection_params)
            adapter.test_connection()

            output_dir = Path(options["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)

            filename = options.get("filename") or self._default_filename(options["name"], options["db_type"])
            output_path = output_dir / filename

            produced_path = Path(adapter.backup(str(output_path), backup_type=options["backup_type"], tables=tables))

            if options["compress"]:
                produced_path = Path(compress_file(str(produced_path), remove_original=True))

            if options.get("encrypt_key"):
                produced_path = Path(encrypt_file(str(produced_path), options["encrypt_key"], remove_original=True))

            final_path = self._store_backup(produced_path, options)
            size_bytes = produced_path.stat().st_size if produced_path.exists() else 0
            checksum = self._sha256(produced_path) if produced_path.exists() else ""

            artifact = BackupArtifact.objects.create(
                backup_job=job,
                file_name=produced_path.name,
                file_path=final_path,
                storage_type=options["storage"],
                size_bytes=size_bytes,
                checksum_sha256=checksum,
                is_compressed=options["compress"],
                is_encrypted=bool(options.get("encrypt_key")),
            )

            finished_at = timezone.now()
            duration = (finished_at - started_at).total_seconds()
            job.status = BackupJob.STATUS_SUCCESS
            job.finished_at = finished_at
            job.duration_seconds = duration
            job.last_error = ""
            job.save(update_fields=["status", "finished_at", "duration_seconds", "last_error", "updated_at"])

            success_msg = (
                f"Backup completed. artifact_id={artifact.id} path='{artifact.file_path}' "
                f"size_bytes={artifact.size_bytes}"
            )
            logger.info(success_msg)
            self.stdout.write(self.style.SUCCESS(success_msg))

            send_slack_notification(options.get("slack_webhook_url"), success_msg)

        except Exception as exc:
            finished_at = timezone.now()
            duration = (finished_at - started_at).total_seconds()

            job.status = BackupJob.STATUS_FAILED
            job.finished_at = finished_at
            job.duration_seconds = duration
            job.last_error = str(exc)
            job.save(update_fields=["status", "finished_at", "duration_seconds", "last_error", "updated_at"])

            logger.exception("Backup failed: %s", exc)
            send_slack_notification(options.get("slack_webhook_url"), f"Backup failed: {exc}")
            raise CommandError(f"Backup failed: {exc}") from exc

    def _default_filename(self, name: str, db_type: str) -> str:
        suffix_map = {
            "sqlite": ".sqlite3",
            "postgres": ".dump",
            "mysql": ".sql",
            "mongo": ".archive",
        }
        safe_name = name.replace(" ", "_").lower()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return f"{safe_name}-{db_type}-{timestamp}{suffix_map.get(db_type, '.bak')}"

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

    def _store_backup(self, local_path: Path, options: dict) -> str:
        storage_type = options["storage"]

        if storage_type == "local":
            backend = LocalStorageBackend()
            return backend.store_file(str(local_path), options["output_dir"])

        if storage_type == "s3":
            bucket = options.get("bucket")
            if not bucket:
                raise AdapterError("--bucket is required for S3 storage")
            backend = S3StorageBackend(bucket=bucket, prefix=options.get("prefix", ""), region=options.get("region"))
            return backend.store_file(str(local_path))

        if storage_type == "gcs":
            bucket = options.get("bucket")
            if not bucket:
                raise AdapterError("--bucket is required for GCS storage")
            backend = GCSStorageBackend(bucket=bucket, prefix=options.get("prefix", ""))
            return backend.store_file(str(local_path))

        if storage_type == "azure":
            container = options.get("container")
            if not container:
                raise AdapterError("--container is required for Azure storage")
            backend = AzureBlobStorageBackend(
                container=container,
                prefix=options.get("prefix", ""),
                connection_string=options.get("azure_connection_string"),
            )
            return backend.store_file(str(local_path))

        raise AdapterError(f"Unsupported storage type: {storage_type}")

    def _redact(self, params: dict) -> dict:
        result = dict(params)
        for key in ["password", "uri", "token", "secret", "azure_connection_string"]:
            if key in result and result[key]:
                result[key] = "***"
        return result

    def _sha256(self, file_path: Path) -> str:
        hasher = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
