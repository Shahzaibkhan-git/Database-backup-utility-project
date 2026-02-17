from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable

from .base import AdapterError, DatabaseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(DatabaseAdapter):
    db_type = "postgres"
    supports_selective_restore = True

    def test_connection(self) -> None:
        self._require_binary("psql")

        command = [
            "psql",
            "--no-password",
            "--tuples-only",
            "--no-align",
            "--command",
            "SELECT 1;",
        ]
        command.extend(self._db_connection_command_parts())

        self._run_command(command, "PostgreSQL connection test")

    def backup(
        self,
        output_path: str,
        backup_type: str = "full",
        tables: Iterable[str] | None = None,
    ) -> str:
        # Incremental/differential currently use full snapshot fallback for PostgreSQL.
        self.effective_backup_type(backup_type)

        self._require_binary("pg_dump")

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        command = [
            "pg_dump",
            "--no-password",
            "--format=custom",
            "--file",
            str(output),
        ]
        command.extend(self._table_args(tables))
        command.extend(self._connection_target_args())

        self._run_command(command, "PostgreSQL backup")
        return str(output)

    def restore(self, backup_file: str, tables: Iterable[str] | None = None) -> None:
        source = Path(backup_file)
        if not source.exists():
            raise AdapterError(f"Backup file not found: {source}")

        if source.suffix.lower() == ".sql":
            if tables:
                raise AdapterError(
                    "Selective restore from plain SQL is not supported. "
                    "Use a .dump file for table-selective restore."
                )
            self._require_binary("psql")
            command = [
                "psql",
                "--no-password",
                "--set",
                "ON_ERROR_STOP=1",
            ]
            command.extend(self._db_connection_command_parts())
            command.extend(["-f", str(source)])
            self._run_command(command, "PostgreSQL restore")
            return

        self._require_binary("pg_restore")

        command = [
            "pg_restore",
            "--no-password",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
        ]
        command.extend(self._table_args(tables))
        command.extend(self._db_connection_command_parts())
        command.append(str(source))

        self._run_command(command, "PostgreSQL restore")

    def _run_command(self, command: list[str], action: str) -> None:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                env=self._command_env(),
                check=False,
            )
        except OSError as exc:
            raise AdapterError(f"{action} failed: {exc}") from exc

        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            if not details:
                details = "Unknown command failure."
            if action == "PostgreSQL restore" and self._is_ignorable_restore_warning(details):
                logger.warning("Ignoring known PostgreSQL restore compatibility warning: %s", details)
                return
            raise AdapterError(f"{action} failed: {details}")

    def _is_ignorable_restore_warning(self, details: str) -> bool:
        normalized = " ".join(details.split())
        has_transaction_timeout = 'unrecognized configuration parameter "transaction_timeout"' in normalized
        has_single_ignored_error = "errors ignored on restore: 1" in normalized
        return has_transaction_timeout and has_single_ignored_error

    def _command_env(self) -> dict[str, str]:
        env = os.environ.copy()
        password = self.connection_params.get("password")
        if password:
            env["PGPASSWORD"] = str(password)
        return env

    def _require_binary(self, binary_name: str) -> None:
        if shutil.which(binary_name):
            return
        raise AdapterError(
            f"'{binary_name}' is required but not found in PATH. "
            "Install PostgreSQL client tools."
        )

    def _connection_target_args(self) -> list[str]:
        uri = self.connection_params.get("uri")
        if uri:
            return [str(uri)]

        database = self.connection_params.get("database")
        if not database:
            raise AdapterError("PostgreSQL requires --database or --uri.")

        args = self._standard_connection_args()
        args.append(str(database))
        return args

    def _db_connection_command_parts(self) -> list[str]:
        uri = self.connection_params.get("uri")
        if uri:
            return ["--dbname", str(uri)]

        database = self.connection_params.get("database")
        if not database:
            raise AdapterError("PostgreSQL requires --database or --uri.")

        args = self._standard_connection_args()
        args.extend(["--dbname", str(database)])
        return args

    def _standard_connection_args(self) -> list[str]:
        args: list[str] = []
        host = self.connection_params.get("host")
        port = self.connection_params.get("port")
        username = self.connection_params.get("username")

        if host:
            args.extend(["--host", str(host)])
        if port:
            args.extend(["--port", str(port)])
        if username:
            args.extend(["--username", str(username)])

        return args

    def _table_args(self, tables: Iterable[str] | None) -> list[str]:
        args: list[str] = []
        if not tables:
            return args

        for table in tables:
            value = str(table).strip()
            if value:
                args.extend(["--table", value])

        return args
