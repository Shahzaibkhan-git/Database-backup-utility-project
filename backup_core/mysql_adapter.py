from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlparse

from .base import AdapterError, DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    db_type = "mysql"

    def __init__(self, connection_params: dict | None = None) -> None:
        super().__init__(connection_params)
        self.params = self._normalized_params()

    def test_connection(self) -> None:
        self._require_binary("mysql")

        command = [
            "mysql",
            "--batch",
            "--skip-column-names",
            "--execute",
            "SELECT 1;",
        ]
        command.extend(self._connection_args(include_database=True))

        self._run_command(command, "MySQL connection test")

    def backup(
        self,
        output_path: str,
        backup_type: str = "full",
        tables: Iterable[str] | None = None,
    ) -> str:
        self.validate_backup_type(backup_type)
        if backup_type != "full":
            raise AdapterError("MySQL adapter currently supports only full backup.")

        self._require_binary("mysqldump")
        database = self._required_database()

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        command = [
            "mysqldump",
            "--single-transaction",
            "--quick",
            "--routines",
            "--events",
            "--triggers",
            "--no-tablespaces",
            "--result-file",
            str(output),
        ]
        command.extend(self._connection_args(include_database=False))
        command.append(database)
        command.extend(self._table_list(tables))

        self._run_command(command, "MySQL backup")
        return str(output)

    def restore(self, backup_file: str, tables: Iterable[str] | None = None) -> None:
        if tables:
            raise AdapterError("Selective restore is not implemented for MySQL yet.")

        source = Path(backup_file)
        if not source.exists():
            raise AdapterError(f"Backup file not found: {source}")

        self._require_binary("mysql")
        self._required_database()

        command = ["mysql"]
        command.extend(self._connection_args(include_database=True))
        self._run_command(command, "MySQL restore", stdin_path=source)

    def _run_command(self, command: list[str], action: str, stdin_path: Path | None = None) -> None:
        try:
            if stdin_path is None:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    env=self._command_env(),
                    check=False,
                )
            else:
                with stdin_path.open("rb") as source_handle:
                    result = subprocess.run(
                        command,
                        stdin=source_handle,
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
            raise AdapterError(f"{action} failed: {details}")

    def _command_env(self) -> dict[str, str]:
        env = os.environ.copy()
        password = self.params.get("password")
        if password:
            env["MYSQL_PWD"] = str(password)
        return env

    def _require_binary(self, binary_name: str) -> None:
        if shutil.which(binary_name):
            return
        raise AdapterError(
            f"'{binary_name}' is required but not found in PATH. "
            "Install MySQL client tools."
        )

    def _connection_args(self, include_database: bool) -> list[str]:
        args: list[str] = []

        host = self.params.get("host")
        port = self.params.get("port")
        username = self.params.get("username")

        if host:
            args.extend(["--host", str(host)])
        if port:
            args.extend(["--port", str(port)])
        if username:
            args.extend(["--user", str(username)])
        if include_database and self.params.get("database"):
            args.extend(["--database", str(self.params["database"])])

        return args

    def _required_database(self) -> str:
        database = self.params.get("database")
        if not database:
            raise AdapterError("MySQL requires --database or --uri with database name.")
        return str(database)

    def _table_list(self, tables: Iterable[str] | None) -> list[str]:
        if not tables:
            return []
        return [str(table).strip() for table in tables if str(table).strip()]

    def _normalized_params(self) -> dict:
        params = dict(self.connection_params)

        uri = params.get("uri")
        if not uri:
            return params

        parsed = urlparse(str(uri))
        if parsed.scheme not in ("mysql", "mariadb"):
            raise AdapterError("MySQL URI must start with mysql:// or mariadb://")

        if "username" not in params and parsed.username:
            params["username"] = unquote(parsed.username)
        if "password" not in params and parsed.password:
            params["password"] = unquote(parsed.password)
        if "host" not in params and parsed.hostname:
            params["host"] = parsed.hostname
        if "port" not in params and parsed.port:
            params["port"] = parsed.port
        if "database" not in params and parsed.path and parsed.path != "/":
            params["database"] = unquote(parsed.path.lstrip("/"))

        return params
