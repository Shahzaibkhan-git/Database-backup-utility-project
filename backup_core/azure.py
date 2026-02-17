from __future__ import annotations

from pathlib import Path


class AzureBlobStorageBackend:
    def __init__(self, container: str, prefix: str = "", connection_string: str | None = None) -> None:
        self.container = container
        self.prefix = prefix.strip("/")
        self.connection_string = connection_string

    def store_file(self, source_path: str, filename: str | None = None) -> str:
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise RuntimeError(
                "Azure upload requires azure-storage-blob. Install it with: pip install azure-storage-blob"
            ) from exc

        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Backup file does not exist: {source}")

        blob_name = filename or source.name
        if self.prefix:
            blob_name = f"{self.prefix}/{blob_name}"

        if not self.connection_string:
            raise RuntimeError("Azure connection string is required for Azure blob uploads.")

        service = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = service.get_container_client(self.container)

        with source.open("rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        return f"azure://{self.container}/{blob_name}"
