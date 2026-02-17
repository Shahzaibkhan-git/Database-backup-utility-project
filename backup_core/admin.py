from django.contrib import admin

from .models import BackupArtifact, BackupJob, RestoreJob, Schedule


@admin.register(BackupJob)
class BackupJobAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "db_type",
        "backup_type",
        "storage_type",
        "status",
        "created_at",
    )
    list_filter = ("db_type", "backup_type", "storage_type", "status")
    search_fields = ("name",)


@admin.register(BackupArtifact)
class BackupArtifactAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "backup_job",
        "file_name",
        "storage_type",
        "size_bytes",
        "is_compressed",
        "is_encrypted",
        "created_at",
    )
    list_filter = ("storage_type", "is_compressed", "is_encrypted")
    search_fields = ("file_name", "file_path")


@admin.register(RestoreJob)
class RestoreJobAdmin(admin.ModelAdmin):
    list_display = ("id", "backup_job", "backup_artifact", "status", "created_at")
    list_filter = ("status",)


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "backup_job",
        "cron_expression",
        "is_active",
        "retry_count",
        "max_retries",
        "next_run_at",
        "lease_expires_at",
    )
    list_filter = ("is_active",)
