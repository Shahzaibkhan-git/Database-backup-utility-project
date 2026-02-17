from __future__ import annotations

import sqlite3
from subprocess import CompletedProcess
import tempfile
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase
from django.test import SimpleTestCase
from django.utils import timezone

from .base import AdapterError
from .mongo_adapter import MongoAdapter
from .mysql_adapter import MySQLAdapter
from .postgres_adapter import PostgresAdapter
from .scheduler import get_next_run_at
from .sqlite_adapter import SQLiteAdapter
from .models import BackupJob, Schedule


class SQLiteAdapterTests(SimpleTestCase):
    def test_backup_creates_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.db"
            backup_path = Path(tmp_dir) / "backup.db"

            conn = sqlite3.connect(source_path)
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT);")
            conn.execute("INSERT INTO sample (name) VALUES ('demo');")
            conn.commit()
            conn.close()

            adapter = SQLiteAdapter({"path": str(source_path)})
            adapter.test_connection()
            result = adapter.backup(str(backup_path))

            self.assertTrue(Path(result).exists())

    def test_restore_replaces_target(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.db"
            backup_path = Path(tmp_dir) / "backup.db"
            target_path = Path(tmp_dir) / "target.db"

            conn = sqlite3.connect(source_path)
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT);")
            conn.execute("INSERT INTO sample (name) VALUES ('from-source');")
            conn.commit()
            conn.close()

            SQLiteAdapter({"path": str(source_path)}).backup(str(backup_path))

            conn = sqlite3.connect(target_path)
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT);")
            conn.execute("INSERT INTO sample (name) VALUES ('old-target');")
            conn.commit()
            conn.close()

            SQLiteAdapter({"path": str(target_path)}).restore(str(backup_path))

            conn = sqlite3.connect(target_path)
            row = conn.execute("SELECT name FROM sample LIMIT 1;").fetchone()
            conn.close()

            self.assertEqual(row[0], "from-source")


class PostgresAdapterTests(SimpleTestCase):
    @patch("backup_core.postgres_adapter.shutil.which", return_value="/usr/bin/psql")
    def test_connection_requires_database_or_uri(self, _mock_which):
        adapter = PostgresAdapter({"host": "localhost", "port": 5432, "username": "demo"})

        with self.assertRaisesMessage(AdapterError, "PostgreSQL requires --database or --uri."):
            adapter.test_connection()

    @patch("backup_core.postgres_adapter.subprocess.run")
    @patch("backup_core.postgres_adapter.shutil.which", return_value="/usr/bin/pg_dump")
    def test_backup_calls_pg_dump(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["pg_dump"], returncode=0, stdout="", stderr="")

        adapter = PostgresAdapter(
            {
                "host": "localhost",
                "port": 5432,
                "username": "demo_user",
                "password": "demo_pass",
                "database": "demo_db",
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "backup.dump"
            result = adapter.backup(str(output_path), tables=["public.users", "public.orders"])

        self.assertEqual(result, str(output_path))
        self.assertEqual(mock_run.call_count, 1)

        command = mock_run.call_args.args[0]
        env = mock_run.call_args.kwargs["env"]

        self.assertEqual(command[0], "pg_dump")
        self.assertIn("--format=custom", command)
        self.assertIn("--file", command)
        self.assertIn("--table", command)
        self.assertIn("public.users", command)
        self.assertIn("public.orders", command)
        self.assertEqual(command[-1], "demo_db")
        self.assertEqual(env["PGPASSWORD"], "demo_pass")

    @patch("backup_core.postgres_adapter.subprocess.run")
    @patch("backup_core.postgres_adapter.shutil.which", return_value="/usr/bin/psql")
    def test_restore_plain_sql_uses_psql(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["psql"], returncode=0, stdout="", stderr="")
        adapter = PostgresAdapter({"database": "demo_db"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_file = Path(tmp_dir) / "restore.sql"
            sql_file.write_text("SELECT 1;", encoding="utf-8")
            adapter.restore(str(sql_file))

        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "psql")
        self.assertIn("--dbname", command)
        self.assertIn("demo_db", command)
        self.assertIn("-f", command)

    def test_restore_plain_sql_with_tables_is_rejected(self):
        adapter = PostgresAdapter({"database": "demo_db"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_file = Path(tmp_dir) / "restore.sql"
            sql_file.write_text("SELECT 1;", encoding="utf-8")

            with self.assertRaisesMessage(
                AdapterError,
                "Selective restore from plain SQL is not supported.",
            ):
                adapter.restore(str(sql_file), tables=["public.users"])

    @patch("backup_core.postgres_adapter.subprocess.run")
    @patch("backup_core.postgres_adapter.shutil.which", return_value="/usr/bin/pg_restore")
    def test_restore_ignores_known_transaction_timeout_warning(self, _mock_which, mock_run):
        warning = (
            'pg_restore: error: could not execute query: ERROR:  unrecognized configuration parameter '
            '"transaction_timeout"\n'
            "Command was: SET transaction_timeout = 0;\n"
            "pg_restore: warning: errors ignored on restore: 1\n"
        )
        mock_run.return_value = CompletedProcess(args=["pg_restore"], returncode=1, stdout="", stderr=warning)

        adapter = PostgresAdapter({"database": "demo_db"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            dump_file = Path(tmp_dir) / "restore.dump"
            dump_file.write_bytes(b"fake")
            adapter.restore(str(dump_file))


class MySQLAdapterTests(SimpleTestCase):
    @patch("backup_core.mysql_adapter.subprocess.run")
    @patch("backup_core.mysql_adapter.shutil.which", return_value="/usr/bin/mysql")
    def test_connection_uses_mysql_command(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mysql"], returncode=0, stdout="", stderr="")

        adapter = MySQLAdapter(
            {
                "host": "localhost",
                "port": 3306,
                "username": "root",
                "password": "secret",
                "database": "mydb",
            }
        )
        adapter.test_connection()

        command = mock_run.call_args.args[0]
        env = mock_run.call_args.kwargs["env"]

        self.assertEqual(command[0], "mysql")
        self.assertIn("--execute", command)
        self.assertIn("SELECT 1;", command)
        self.assertIn("--database", command)
        self.assertIn("mydb", command)
        self.assertEqual(env["MYSQL_PWD"], "secret")

    @patch("backup_core.mysql_adapter.subprocess.run")
    @patch("backup_core.mysql_adapter.shutil.which", return_value="/usr/bin/mysqldump")
    def test_backup_calls_mysqldump_with_tables(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mysqldump"], returncode=0, stdout="", stderr="")

        adapter = MySQLAdapter(
            {
                "host": "localhost",
                "port": 3306,
                "username": "root",
                "password": "secret",
                "database": "mydb",
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "backup.sql"
            result = adapter.backup(str(output_path), tables=["users", "orders"])

        self.assertEqual(result, str(output_path))
        command = mock_run.call_args.args[0]

        self.assertEqual(command[0], "mysqldump")
        self.assertIn("--result-file", command)
        self.assertIn("mydb", command)
        self.assertIn("users", command)
        self.assertIn("orders", command)

    @patch("backup_core.mysql_adapter.subprocess.run")
    @patch("backup_core.mysql_adapter.shutil.which", return_value="/usr/bin/mysql")
    def test_restore_uses_mysql_stdin(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mysql"], returncode=0, stdout="", stderr="")
        adapter = MySQLAdapter({"database": "mydb"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_file = Path(tmp_dir) / "restore.sql"
            sql_file.write_text("SELECT 1;", encoding="utf-8")
            adapter.restore(str(sql_file))

        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "mysql")
        self.assertIn("--database", command)
        self.assertIn("mydb", command)
        self.assertIsNotNone(mock_run.call_args.kwargs.get("stdin"))

    def test_restore_tables_is_rejected(self):
        adapter = MySQLAdapter({"database": "mydb"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_file = Path(tmp_dir) / "restore.sql"
            sql_file.write_text("SELECT 1;", encoding="utf-8")

            with self.assertRaisesMessage(AdapterError, "Selective restore is not implemented for MySQL yet."):
                adapter.restore(str(sql_file), tables=["users"])

    def test_uri_parsing(self):
        adapter = MySQLAdapter({"uri": "mysql://demo:secret@localhost:3306/mydb"})
        self.assertEqual(adapter.params["username"], "demo")
        self.assertEqual(adapter.params["password"], "secret")
        self.assertEqual(adapter.params["host"], "localhost")
        self.assertEqual(adapter.params["port"], 3306)
        self.assertEqual(adapter.params["database"], "mydb")


class MongoAdapterTests(SimpleTestCase):
    @patch("backup_core.mongo_adapter.subprocess.run")
    @patch("backup_core.mongo_adapter.shutil.which", side_effect=["/usr/bin/mongosh"])
    def test_connection_uses_mongosh_when_available(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mongosh"], returncode=0, stdout="", stderr="")
        adapter = MongoAdapter({"uri": "mongodb://localhost:27017/mydb"})

        adapter.test_connection()

        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "mongosh")
        self.assertIn("mongodb://localhost:27017/mydb", command)
        self.assertIn("--eval", command)

    @patch("backup_core.mongo_adapter.subprocess.run")
    @patch("backup_core.mongo_adapter.shutil.which", side_effect=[None, "/usr/bin/mongodump"])
    def test_connection_falls_back_to_mongodump(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mongodump"], returncode=0, stdout="", stderr="")
        adapter = MongoAdapter({"database": "mydb"})

        adapter.test_connection()

        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "mongodump")
        self.assertIn("--db", command)
        self.assertIn("mydb", command)

    @patch("backup_core.mongo_adapter.subprocess.run")
    @patch("backup_core.mongo_adapter.shutil.which", return_value="/usr/bin/mongodump")
    def test_backup_calls_mongodump_with_nsinclude(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mongodump"], returncode=0, stdout="", stderr="")
        adapter = MongoAdapter({"uri": "mongodb://localhost:27017/mydb"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "backup.archive"
            result = adapter.backup(str(output_path), tables=["users", "orders"])

        self.assertEqual(result, str(output_path))
        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "mongodump")
        self.assertIn(f"--archive={output_path}", command)
        self.assertIn("--nsInclude=mydb.users", command)
        self.assertIn("--nsInclude=mydb.orders", command)

    @patch("backup_core.mongo_adapter.subprocess.run")
    @patch("backup_core.mongo_adapter.shutil.which", return_value="/usr/bin/mongorestore")
    def test_restore_calls_mongorestore(self, _mock_which, mock_run):
        mock_run.return_value = CompletedProcess(args=["mongorestore"], returncode=0, stdout="", stderr="")
        adapter = MongoAdapter({"database": "mydb"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "backup.archive"
            archive_path.write_bytes(b"fake")
            adapter.restore(str(archive_path), tables=["users"])

        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "mongorestore")
        self.assertIn(f"--archive={archive_path}", command)
        self.assertIn("--drop", command)
        self.assertIn("--nsInclude=mydb.users", command)

    @patch("backup_core.mongo_adapter.shutil.which", return_value="/usr/bin/mongodump")
    def test_selective_backup_requires_database(self, _mock_which):
        adapter = MongoAdapter({"host": "localhost"})

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "backup.archive"
            with self.assertRaisesMessage(AdapterError, "MongoDB requires --database or --uri with database name."):
                adapter.backup(str(archive_path), tables=["users"])

    def test_uri_parsing(self):
        adapter = MongoAdapter({"uri": "mongodb://demo:secret@localhost:27017/mydb"})
        self.assertEqual(adapter.params["username"], "demo")
        self.assertEqual(adapter.params["password"], "secret")
        self.assertEqual(adapter.params["host"], "localhost")
        self.assertEqual(adapter.params["port"], 27017)
        self.assertEqual(adapter.params["database"], "mydb")


class SchedulerCronTests(SimpleTestCase):
    def test_next_run_every_five_minutes(self):
        base = timezone.make_aware(datetime(2026, 2, 17, 10, 2, 15))
        next_run = get_next_run_at("*/5 * * * *", after=base)
        self.assertEqual(next_run.minute, 5)
        self.assertEqual(next_run.hour, 10)

    def test_next_run_hourly_alias(self):
        base = timezone.make_aware(datetime(2026, 2, 17, 10, 59, 30))
        next_run = get_next_run_at("@hourly", after=base)
        self.assertEqual(next_run.hour, 11)
        self.assertEqual(next_run.minute, 0)

    def test_invalid_cron_raises(self):
        with self.assertRaisesMessage(ValueError, "Cron expression must contain 5 fields."):
            get_next_run_at("bad cron")


class SchedulerCommandTests(TestCase):
    def setUp(self):
        self.template = BackupJob.objects.create(
            name="template-job",
            db_type="sqlite",
            backup_type="full",
            connection_params={"path": "db.sqlite3"},
            storage_type="local",
            destination="backups",
        )
        self.schedule = Schedule.objects.create(
            backup_job=self.template,
            cron_expression="*/5 * * * *",
            is_active=True,
            next_run_at=timezone.now() - timezone.timedelta(minutes=1),
        )

    @patch("backup_core.management.commands.run_scheduler.call_command")
    def test_once_mode_executes_due_schedule(self, mock_call_command):
        out = StringIO()
        call_command("run_scheduler", once=True, stdout=out)

        self.schedule.refresh_from_db()
        self.assertIsNotNone(self.schedule.last_run_at)
        self.assertIsNotNone(self.schedule.next_run_at)
        self.assertGreater(self.schedule.next_run_at, self.schedule.last_run_at)
        mock_call_command.assert_called_once()
        self.assertEqual(mock_call_command.call_args.args[0], "backup_db")

    @patch("backup_core.management.commands.run_scheduler.call_command")
    def test_dry_run_does_not_update_schedule(self, mock_call_command):
        old_last_run = self.schedule.last_run_at
        old_next_run = self.schedule.next_run_at

        call_command("run_scheduler", once=True, dry_run=True)
        self.schedule.refresh_from_db()

        self.assertEqual(self.schedule.last_run_at, old_last_run)
        self.assertEqual(self.schedule.next_run_at, old_next_run)
        mock_call_command.assert_not_called()

    def test_create_schedule_command(self):
        out = StringIO()
        call_command(
            "create_schedule",
            backup_job_id=self.template.id,
            cron="*/10 * * * *",
            due_now=True,
            stdout=out,
        )

        created = Schedule.objects.filter(backup_job=self.template).exclude(id=self.schedule.id).first()
        self.assertIsNotNone(created)
        self.assertIn("Schedule created", out.getvalue())

    def test_list_schedules_command(self):
        out = StringIO()
        call_command("list_schedules", limit=10, stdout=out)
        text = out.getvalue()
        self.assertIn("backup_job_id=", text)
        self.assertIn("cron='*/5 * * * *'", text)
