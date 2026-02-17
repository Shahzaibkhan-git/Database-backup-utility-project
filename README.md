# Project Page URL
https://roadmap.sh/projects/database-backup-utility

# Database Backup Utility (Django CLI)

Command-line backup and restore utility built with Django.

Current status:
- SQLite full backup and restore: implemented
- PostgreSQL full backup and restore via `pg_dump`/`pg_restore`: implemented
- MySQL full backup and restore via `mysqldump`/`mysql`: implemented
- MongoDB full backup and restore via `mongodump`/`mongorestore`: implemented
- Compression (`.gz`): implemented
- Encryption (`.enc` using `cryptography`): implemented
- Local storage: implemented
- S3/GCS/Azure upload backends: implemented

## 1) Architecture in One Minute

- `control.sqlite3`:
  Django metadata DB (stores `BackupJob`, `BackupArtifact`, `RestoreJob`, `Schedule`).
- `db.sqlite3`:
  Default target SQLite DB you backup/restore.
- Management commands:
  - `backup_db`
  - `restore_db`
  - `test_db_connection`
  - `list_backups`
  - `create_schedule`
  - `list_schedules`
  - `system_status`
  - `run_scheduler`

Why two DB files?
- If metadata and target are the same file, restoring can overwrite metadata rows (artifact IDs disappear).
- Separate files keep backup history stable.

PostgreSQL prerequisites:
- `psql`, `pg_dump`, and `pg_restore` must be installed and available in `PATH`.

MySQL prerequisites:
- `mysql` and `mysqldump` must be installed and available in `PATH`.

MongoDB prerequisites:
- `mongodump` and `mongorestore` must be installed and available in `PATH`.
- `mongosh` is optional but recommended for clean connection testing.

## 2) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional cloud providers:

```bash
pip install -r requirements-optional.txt
```

Run migrations for metadata DB:

```bash
./.venv/bin/python manage.py migrate
```

## 3) Defaults and Environment Variables

Defaults:
- Metadata DB: `control.sqlite3`
- Target SQLite DB: `db.sqlite3`
- Backup output dir: `backups/`
- Log file: `logs/backup.log`

Optional overrides:

```bash
export DJANGO_SQLITE_PATH=/absolute/path/to/control.sqlite3
export TARGET_SQLITE_DB_PATH=/absolute/path/to/your_target.sqlite3
export DJANGO_DEBUG=1
```

## 4) Command Usage

Test connectivity:

```bash
./.venv/bin/python manage.py test_db_connection --db-type sqlite --db-path db.sqlite3
```

PostgreSQL connectivity check:

```bash
./.venv/bin/python manage.py test_db_connection \
  --db-type postgres \
  --host localhost \
  --port 5432 \
  --username postgres \
  --password "<YOUR_POSTGRES_PASSWORD>" \
  --database mydb
```

MySQL connectivity check:

```bash
./.venv/bin/python manage.py test_db_connection \
  --db-type mysql \
  --host localhost \
  --port 3306 \
  --username root \
  --password "<YOUR_MYSQL_PASSWORD>" \
  --database mydb
```

MongoDB connectivity check:

```bash
./.venv/bin/python manage.py test_db_connection \
  --db-type mongo \
  --uri "mongodb://localhost:27017/mydb"
```

MongoDB backup:

```bash
./.venv/bin/python manage.py backup_db \
  --name mongo-daily \
  --db-type mongo \
  --uri "mongodb://localhost:27017/mydb" \
  --output-dir backups
```

MongoDB restore:

```bash
./.venv/bin/python manage.py restore_db \
  --backup-file backups/mongo-daily-mongo-<timestamp>.archive \
  --db-type mongo \
  --uri "mongodb://localhost:27017/mydb"
```

Create backup:

```bash
./.venv/bin/python manage.py backup_db \
  --name daily \
  --db-type sqlite \
  --db-path db.sqlite3 \
  --output-dir backups \
  --compress
```

List artifacts:

```bash
./.venv/bin/python manage.py list_backups --limit 20
```

Run scheduler once (process due schedules and exit):

```bash
./.venv/bin/python manage.py run_scheduler --once
```

Create schedule from an existing backup job:

```bash
./.venv/bin/python manage.py create_schedule \
  --backup-job-id 5 \
  --cron "*/5 * * * *" \
  --due-now
```

List schedules:

```bash
./.venv/bin/python manage.py list_schedules --active-only
```

Show overall system health:

```bash
./.venv/bin/python manage.py system_status
```

Run scheduler continuously every 60s:

```bash
./.venv/bin/python manage.py run_scheduler --interval-seconds 60
```

Run scheduler via Celery Beat (recommended for production):

```bash
# Optional: change how often beat triggers scheduler task (default 60s)
export BACKUP_SCHEDULER_BEAT_INTERVAL_SECONDS=60

# Start worker
./.venv/bin/celery -A dbbackup worker -l info

# Start beat (in another terminal)
./.venv/bin/celery -A dbbackup beat -l info
```

What Beat does here:
- Every `BACKUP_SCHEDULER_BEAT_INTERVAL_SECONDS`, Beat queues `backup_core.tasks.run_scheduler_once`.
- That task runs one pass of your existing `Schedule` rows (same logic as `manage.py run_scheduler --once`).
- The task runs scheduler in quiet mode to avoid noisy `WARNING/MainProcess` stdout lines.

Restore from artifact id:

```bash
./.venv/bin/python manage.py restore_db \
  --artifact-id 1 \
  --db-type sqlite \
  --db-path db.sqlite3
```

Restore from explicit backup file (recommended when artifact IDs may have changed):

```bash
./.venv/bin/python manage.py restore_db \
  --backup-file backups/your-file.sqlite3.gz \
  --db-type sqlite \
  --db-path db.sqlite3
```

## 5) Common Errors

`BackupArtifact with id=... not found`
- Your metadata DB currently does not contain that row.
- Run `list_backups` first and use a current id.
- Or restore using `--backup-file` directly.

`attempt to write a readonly database`
- Usually happens when restoring into the same SQLite file used by Django metadata.
- This project defaults to separate metadata (`control.sqlite3`) and target (`db.sqlite3`) to prevent that.

## 6) Project Structure

- `backup_core/models.py`: metadata models
- `backup_core/base.py`: adapter interface and factory
- `backup_core/sqlite_adapter.py`: working SQLite backup/restore
- `backup_core/postgres_adapter.py`: PostgreSQL backup/restore via `pg_dump` and `pg_restore`
- `backup_core/mysql_adapter.py`: MySQL backup/restore via `mysqldump` and `mysql`
- `backup_core/mongo_adapter.py`: MongoDB backup/restore via `mongodump` and `mongorestore`
- `backup_core/management/commands/*.py`: CLI commands
- `backup_core/compression.py`: gzip utilities
- `backup_core/encryption.py`: file encryption/decryption
- `backup_core/local.py`, `s3.py`, `gcs.py`, `azure.py`: storage backends
- `backup_core/scheduler.py`: schedule selection and cron next-run calculation

## 7) Tested Flows

Validated in this project environment:
- SQLite: connection test, full backup, list artifacts, restore.
- PostgreSQL: connection test, full backup, restore (including compatibility handling for `transaction_timeout` warning).
- Scheduler: dry run detects due schedules, actual run executes `backup_db` and creates `BackupArtifact`.

## 8) Example Scheduler Run Output

```text
./.venv/bin/python manage.py run_scheduler --once --dry-run
Scheduler started.
2026-02-17 10:55:02,050 | INFO | backup_core.scheduler | [DRY RUN] schedule_id=1 backup_job_id=5 cron='*/5 * * * *' next_run=2026-02-17T11:00:00+00:00
Scheduler finished. processed=1

./.venv/bin/python manage.py run_scheduler --once
Scheduler started.
2026-02-17 10:55:10,588 | INFO | backup_core.scheduler | Running schedule_id=1 backup_job_id=5 db_type=sqlite storage=local
2026-02-17 10:55:10,641 | INFO | backup_core.backup | Backup completed. artifact_id=4 path='backups/sqlite-scheduled-sqlite-20260217T105510Z.sqlite3.gz' size_bytes=5485
Scheduler finished. processed=1
```

## 9) Next Implementation Milestones

1. Incremental/differential support per database.
2. Advanced scheduler features (retry policy, lock/lease, backoff).
