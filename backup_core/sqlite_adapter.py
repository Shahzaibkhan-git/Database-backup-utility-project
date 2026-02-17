from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable

from django.conf import settings

from .base import AdapterError, DatabaseAdapter


class SQLiteAdapter(DatabaseAdapter):
    db_type = "sqlite"

    def _database_path(self) -> Path:
        db_path = self.connection_params.get("path")

        if not db_path:
            db_path = getattr(settings, "TARGET_SQLITE_DB_PATH", settings.DATABASES["default"]["NAME"])

        path = Path(db_path)
        return path

    def test_connection(self) -> None:
        db_path = self._database_path()
        allow_create = bool(self.connection_params.get("allow_create", False))

        if str(db_path) != ":memory:" and not db_path.exists() and not allow_create:
            raise AdapterError(f"SQLite database file does not exist: {db_path}")

        try:
            if str(db_path) != ":memory:" and allow_create:
                db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path), timeout=10)
            conn.execute("SELECT 1;")
            conn.close()
        except sqlite3.Error as exc:
            raise AdapterError(f"Failed to connect to SQLite database: {exc}") from exc

    def backup(
        self,
        output_path: str,
        backup_type: str = "full",
        tables: Iterable[str] | None = None,
    ) -> str:
        # Incremental/differential currently use full snapshot fallback for SQLite.
        self.effective_backup_type(backup_type)

        if tables:
            raise AdapterError("Selective backup is not implemented for SQLite yet.")

        source_path = self._database_path()
        if not source_path.exists():
            raise AdapterError(f"Cannot backup. Source database not found: {source_path}")

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        try:
            src_conn = sqlite3.connect(str(source_path))
            dst_conn = sqlite3.connect(str(output))
            src_conn.backup(dst_conn)
            dst_conn.close()
            src_conn.close()
        except sqlite3.Error as exc:
            raise AdapterError(f"SQLite backup failed: {exc}") from exc

        return str(output)

    def restore(self, backup_file: str, tables: Iterable[str] | None = None) -> None:
        if tables:
            raise AdapterError("Selective restore is not implemented for SQLite yet.")

        backup_path = Path(backup_file)
        if not backup_path.exists():
            raise AdapterError(f"Backup file not found: {backup_path}")

        target_path = self._database_path()
        target_path.parent.mkdir(parents=True, exist_ok=True)

        if backup_path.resolve() == target_path.resolve():
            return

        tmp_fd, tmp_path = tempfile.mkstemp(
            prefix="sqlite_restore_",
            suffix=".db",
            dir=str(target_path.parent),
        )
        os.close(tmp_fd)

        try:
            src_conn = sqlite3.connect(str(backup_path))
            dst_conn = sqlite3.connect(tmp_path)
            src_conn.backup(dst_conn)
            dst_conn.close()
            src_conn.close()
            os.replace(tmp_path, target_path)
        except sqlite3.Error as exc:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise AdapterError(f"SQLite restore failed: {exc}") from exc
