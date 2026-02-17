from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable


class AdapterError(Exception):
    """Raised for adapter-level backup and restore issues."""


class DatabaseAdapter(ABC):
    db_type = "unknown"
    supports_incremental = False
    supports_differential = False
    fallback_incremental_to_full = True
    fallback_differential_to_full = True
    supports_selective_restore = False

    def __init__(self, connection_params: dict | None = None) -> None:
        self.connection_params = connection_params or {}

    @abstractmethod
    def test_connection(self) -> None:
        """Validate database connectivity and raise AdapterError on failure."""

    @abstractmethod
    def backup(
        self,
        output_path: str,
        backup_type: str = "full",
        tables: Iterable[str] | None = None,
    ) -> str:
        """Create backup and return final local path of produced backup file."""

    @abstractmethod
    def restore(self, backup_file: str, tables: Iterable[str] | None = None) -> None:
        """Restore database from backup file."""

    def validate_backup_type(self, backup_type: str) -> None:
        allowed = {"full", "incremental", "differential"}
        if backup_type not in allowed:
            raise AdapterError(f"Unsupported backup type '{backup_type}'.")

        if backup_type == "incremental" and not self.supports_incremental and not self.fallback_incremental_to_full:
            raise AdapterError(f"{self.db_type} adapter does not support incremental backup yet.")

        if backup_type == "differential" and not self.supports_differential and not self.fallback_differential_to_full:
            raise AdapterError(f"{self.db_type} adapter does not support differential backup yet.")

    def effective_backup_type(self, backup_type: str) -> str:
        self.validate_backup_type(backup_type)
        if backup_type == "incremental" and not self.supports_incremental:
            return "full"
        if backup_type == "differential" and not self.supports_differential:
            return "full"
        return backup_type


def get_adapter(db_type: str, connection_params: dict | None = None) -> DatabaseAdapter:
    db_type = (db_type or "").lower().strip()

    if db_type == "sqlite":
        from .sqlite_adapter import SQLiteAdapter

        return SQLiteAdapter(connection_params)

    if db_type == "postgres":
        from .postgres_adapter import PostgresAdapter

        return PostgresAdapter(connection_params)

    if db_type == "mysql":
        from .mysql_adapter import MySQLAdapter

        return MySQLAdapter(connection_params)

    if db_type == "mongo":
        from .mongo_adapter import MongoAdapter

        return MongoAdapter(connection_params)

    raise AdapterError(f"Unsupported db type '{db_type}'.")
