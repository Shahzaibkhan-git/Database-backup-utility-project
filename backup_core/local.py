from __future__ import annotations

import shutil
from pathlib import Path


class LocalStorageBackend:
    def store_file(self, source_path: str, destination_dir: str, filename: str | None = None) -> str:
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Backup file does not exist: {source}")

        destination = Path(destination_dir)
        destination.mkdir(parents=True, exist_ok=True)

        final_name = filename or source.name
        target = destination / final_name

        if source.resolve() != target.resolve():
            shutil.copy2(source, target)

        return str(target)
