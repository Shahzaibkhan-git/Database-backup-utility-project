from django.db import models


class BackupJob(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
    ]

    DB_TYPE_CHOICES = [
        ("sqlite", "SQLite"),
        ("postgres", "PostgreSQL"),
        ("mysql", "MySQL"),
        ("mongo", "MongoDB"),
    ]

    BACKUP_TYPE_CHOICES = [
        ("full", "Full"),
        ("incremental", "Incremental"),
        ("differential", "Differential"),
    ]

    name = models.CharField(max_length=255)
    db_type = models.CharField(max_length=20, choices=DB_TYPE_CHOICES, default="sqlite")
    backup_type = models.CharField(max_length=20, choices=BACKUP_TYPE_CHOICES, default="full")
    connection_params = models.JSONField(default=dict, blank=True)
    storage_type = models.CharField(max_length=20, default="local")
    destination = models.CharField(max_length=1024, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    is_compressed = models.BooleanField(default=False)
    is_encrypted = models.BooleanField(default=False)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.name} ({self.db_type})"


class BackupArtifact(models.Model):
    backup_job = models.ForeignKey(BackupJob, on_delete=models.CASCADE, related_name="artifacts")
    file_name = models.CharField(max_length=255)
    file_path = models.CharField(max_length=1024)
    storage_type = models.CharField(max_length=20, default="local")
    size_bytes = models.BigIntegerField(default=0)
    checksum_sha256 = models.CharField(max_length=64, blank=True)
    is_compressed = models.BooleanField(default=False)
    is_encrypted = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return self.file_name


class RestoreJob(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
    ]

    backup_job = models.ForeignKey(
        BackupJob,
        on_delete=models.SET_NULL,
        related_name="restore_jobs",
        null=True,
        blank=True,
    )
    backup_artifact = models.ForeignKey(
        BackupArtifact,
        on_delete=models.SET_NULL,
        related_name="restore_jobs",
        null=True,
        blank=True,
    )
    target_params = models.JSONField(default=dict, blank=True)
    selected_tables = models.JSONField(default=list, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"RestoreJob #{self.id} ({self.status})"


class Schedule(models.Model):
    backup_job = models.ForeignKey(BackupJob, on_delete=models.CASCADE, related_name="schedules")
    cron_expression = models.CharField(max_length=120)
    is_active = models.BooleanField(default=True)
    max_retries = models.PositiveIntegerField(default=3)
    retry_backoff_seconds = models.PositiveIntegerField(default=60)
    retry_count = models.PositiveIntegerField(default=0)
    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.backup_job.name}: {self.cron_expression}"
