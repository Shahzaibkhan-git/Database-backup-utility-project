from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from backup_core.base import AdapterError, get_adapter


class Command(BaseCommand):
    help = "Test connectivity for supported databases."

    def add_arguments(self, parser):
        parser.add_argument("--db-type", default="sqlite", choices=["sqlite", "postgres", "mysql", "mongo"])
        parser.add_argument("--db-path", help="SQLite database file path")
        parser.add_argument("--host")
        parser.add_argument("--port", type=int)
        parser.add_argument("--username")
        parser.add_argument("--password")
        parser.add_argument("--database")
        parser.add_argument("--uri")

    def handle(self, *args, **options):
        connection_params = self._build_connection_params(options)

        try:
            adapter = get_adapter(options["db_type"], connection_params)
            adapter.test_connection()
        except AdapterError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Connection test successful."))

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
