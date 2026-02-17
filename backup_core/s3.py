from __future__ import annotations

from pathlib import Path


class S3StorageBackend:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region = region

    def store_file(self, source_path: str, filename: str | None = None) -> str:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("S3 upload requires boto3. Install it with: pip install boto3") from exc

        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Backup file does not exist: {source}")

        object_name = filename or source.name
        key = f"{self.prefix}/{object_name}" if self.prefix else object_name

        client = boto3.client("s3", region_name=self.region)
        client.upload_file(str(source), self.bucket, key)
        return f"s3://{self.bucket}/{key}"
