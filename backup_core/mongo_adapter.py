from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlparse

from .base import AdapterError, DatabaseAdapter


class MongoAdapter(DatabaseAdapter):
    db_type = "mongo"
    supports_selective_restore = True

    def __init__(self, connection_params: dict | None = None) -> None:
        super().__init__(connection_params)
        self.params = self._normalized_params()

    def test_connection(self) -> None:
        mongosh_path = shutil.which("mongosh")
        if mongosh_path:
            command = [
                "mongosh",
                "--quiet",
                "--eval",
                "db.runCommand({ ping: 1 })",
            ]
            command.extend(self._connection_target_args())
            self._run_command(command, "MongoDB connection test")
            return

        self._require_binary("mongodump")
        with tempfile.NamedTemporaryFile(prefix="mongo_connect_", suffix=".archive", delete=False) as tmp:
            archive_path = Path(tmp.name)

        try:
            command = [
                "mongodump",
                f"--archive={archive_path}",
                "--quiet",
            ]
            command.extend(self._connection_args())
            database = self.params.get("database")
            if database:
                command.extend(["--db", str(database)])
            self._run_command(command, "MongoDB connection test")
        finally:
            archive_path.unlink(missing_ok=True)

    def backup(
        self,
        output_path: str,
        backup_type: str = "full",
        tables: Iterable[str] | None = None,
    ) -> str:
        self.validate_backup_type(backup_type)
        if backup_type != "full":
            raise AdapterError("MongoDB adapter currently supports only full backup.")

        self._require_binary("mongodump")
        database = self._required_database()

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        command = [
            "mongodump",
            f"--archive={output}",
            "--quiet",
        ]
        command.extend(self._connection_args())
        command.extend(["--db", database])
        command.extend(self._namespace_filters(database, tables))

        self._run_command(command, "MongoDB backup")
        return str(output)

    def restore(self, backup_file: str, tables: Iterable[str] | None = None) -> None:
        source = Path(backup_file)
        if not source.exists():
            raise AdapterError(f"Backup file not found: {source}")

        self._require_binary("mongorestore")

        command = [
            "mongorestore",
            f"--archive={source}",
            "--drop",
            "--quiet",
        ]
        command.extend(self._connection_args())

        if tables:
            database = self._required_database()
            command.extend(self._namespace_filters(database, tables))

        self._run_command(command, "MongoDB restore")

    def _run_command(self, command: list[str], action: str) -> None:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
                check=False,
            )
        except OSError as exc:
            raise AdapterError(f"{action} failed: {exc}") from exc

        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            if not details:
                details = "Unknown command failure."
            raise AdapterError(f"{action} failed: {details}")

    def _require_binary(self, binary_name: str) -> None:
        if shutil.which(binary_name):
            return
        raise AdapterError(
            f"'{binary_name}' is required but not found in PATH. "
            "Install MongoDB Database Tools."
        )

    def _connection_target_args(self) -> list[str]:
        uri = self.params.get("uri")
        if uri:
            return [str(uri)]

        host = self.params.get("host", "localhost")
        port = self.params.get("port")

        target = str(host)
        if port:
            target = f"{target}:{port}"

        database = self.params.get("database")
        if database:
            target = f"{target}/{database}"

        args = [target]

        username = self.params.get("username")
        password = self.params.get("password")
        if username:
            args.extend(["--username", str(username)])
        if password:
            args.extend(["--password", str(password)])

        return args

    def _connection_args(self) -> list[str]:
        uri = self.params.get("uri")
        if uri:
            return [f"--uri={uri}"]

        args: list[str] = []
        host = self.params.get("host")
        port = self.params.get("port")
        username = self.params.get("username")
        password = self.params.get("password")

        if host:
            args.extend(["--host", str(host)])
        if port:
            args.extend(["--port", str(port)])
        if username:
            args.extend(["--username", str(username)])
        if password:
            args.extend(["--password", str(password)])

        return args

    def _required_database(self) -> str:
        database = self.params.get("database")
        if not database:
            raise AdapterError("MongoDB requires --database or --uri with database name.")
        return str(database)

    def _namespace_filters(self, database: str, collections: Iterable[str] | None) -> list[str]:
        if not collections:
            return []

        args: list[str] = []
        for collection in collections:
            name = str(collection).strip()
            if not name:
                continue
            args.append(f"--nsInclude={database}.{name}")
        return args

    def _normalized_params(self) -> dict:
        params = dict(self.connection_params)

        uri = params.get("uri")
        if not uri:
            return params

        parsed = urlparse(str(uri))
        if parsed.scheme not in ("mongodb", "mongodb+srv"):
            raise AdapterError("MongoDB URI must start with mongodb:// or mongodb+srv://")

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
