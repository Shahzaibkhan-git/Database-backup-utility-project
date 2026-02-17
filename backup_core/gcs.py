from __future__ import annotations

from pathlib import Path


class GCSStorageBackend:
    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def store_file(self, source_path: str, filename: str | None = None) -> str:
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError(
                "GCS upload requires google-cloud-storage. Install it with: pip install google-cloud-storage"
            ) from exc

        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Backup file does not exist: {source}")

        object_name = filename or source.name
        blob_name = f"{self.prefix}/{object_name}" if self.prefix else object_name

        client = storage.Client()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(source))

        return f"gs://{self.bucket}/{blob_name}"
