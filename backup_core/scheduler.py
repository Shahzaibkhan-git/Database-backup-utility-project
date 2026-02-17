from __future__ import annotations

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .models import Schedule


def get_due_schedules(now=None):
    now = now or timezone.now()
    return (
        Schedule.objects.filter(is_active=True)
        .filter(Q(next_run_at__isnull=True) | Q(next_run_at__lte=now))
        .filter(Q(lease_expires_at__isnull=True) | Q(lease_expires_at__lte=now))
    )


def claim_schedule(schedule_id: int, lease_seconds: int = 300, now=None) -> Schedule | None:
    now = now or timezone.now()
    lease_until = now + timezone.timedelta(seconds=max(int(lease_seconds), 1))

    with transaction.atomic():
        updated = (
            Schedule.objects.filter(id=schedule_id, is_active=True)
            .filter(Q(lease_expires_at__isnull=True) | Q(lease_expires_at__lte=now))
            .update(lease_expires_at=lease_until)
        )
        if updated == 0:
            return None
        return Schedule.objects.select_related("backup_job").get(id=schedule_id)


def mark_schedule_ran(schedule: Schedule, next_run_at=None):
    schedule.last_run_at = timezone.now()
    schedule.next_run_at = next_run_at
    schedule.retry_count = 0
    schedule.last_error = ""
    schedule.lease_expires_at = None
    schedule.save(update_fields=["last_run_at", "next_run_at", "retry_count", "last_error", "lease_expires_at"])


def mark_schedule_failed(schedule: Schedule, error_message: str, next_run_at=None, now=None) -> dict:
    now = now or timezone.now()
    attempt = schedule.retry_count + 1
    max_retries = max(int(schedule.max_retries), 0)

    if attempt <= max_retries:
        delay_seconds = _retry_delay_seconds(schedule.retry_backoff_seconds, attempt)
        retry_at = now + timezone.timedelta(seconds=delay_seconds)
        schedule.retry_count = attempt
        schedule.next_run_at = retry_at
        state = "retrying"
    else:
        # Retries exhausted for this run window; move to next cron tick.
        schedule.retry_count = 0
        schedule.next_run_at = next_run_at
        state = "next_cron"
        delay_seconds = 0

    schedule.last_error = (error_message or "").strip()[:4000]
    schedule.lease_expires_at = None
    schedule.save(update_fields=["retry_count", "next_run_at", "last_error", "lease_expires_at"])

    return {
        "state": state,
        "attempt": attempt,
        "max_retries": max_retries,
        "delay_seconds": delay_seconds,
        "next_run_at": schedule.next_run_at,
    }


def _retry_delay_seconds(base_seconds: int, attempt: int) -> int:
    base = max(int(base_seconds), 1)
    exponent = max(int(attempt) - 1, 0)
    # Exponential backoff capped at 1 hour.
    return min(base * (2**exponent), 3600)


def get_next_run_at(cron_expression: str, after=None):
    """Compute next run for a 5-field cron expression (minute hour dom month dow)."""
    minute_set, hour_set, dom_set, month_set, dow_set, dom_any, dow_any = _parse_cron_expression(cron_expression)
    after = after or timezone.now()

    cursor = after.replace(second=0, microsecond=0) + timezone.timedelta(minutes=1)
    max_minutes = 60 * 24 * 366  # one year search window

    for _ in range(max_minutes):
        cron_dow = (cursor.weekday() + 1) % 7  # cron: Sunday=0
        if cursor.month not in month_set:
            cursor += timezone.timedelta(minutes=1)
            continue
        if cursor.hour not in hour_set:
            cursor += timezone.timedelta(minutes=1)
            continue
        if cursor.minute not in minute_set:
            cursor += timezone.timedelta(minutes=1)
            continue
        if not _day_matches(cursor.day, cron_dow, dom_set, dow_set, dom_any, dow_any):
            cursor += timezone.timedelta(minutes=1)
            continue
        return cursor

    raise ValueError(f"Could not compute next run for cron expression: {cron_expression}")


def _day_matches(day, cron_dow, dom_set, dow_set, dom_any, dow_any):
    dom_match = day in dom_set
    dow_match = cron_dow in dow_set

    if dom_any and dow_any:
        return True
    if dom_any:
        return dow_match
    if dow_any:
        return dom_match
    return dom_match or dow_match


def _parse_cron_expression(expression: str):
    aliases = {
        "@yearly": "0 0 1 1 *",
        "@annually": "0 0 1 1 *",
        "@monthly": "0 0 1 * *",
        "@weekly": "0 0 * * 0",
        "@daily": "0 0 * * *",
        "@midnight": "0 0 * * *",
        "@hourly": "0 * * * *",
    }

    expression = (expression or "").strip()
    expression = aliases.get(expression, expression)
    parts = expression.split()
    if len(parts) != 5:
        raise ValueError("Cron expression must contain 5 fields.")

    minute_any, minute_set = _parse_cron_field(parts[0], 0, 59, "minute")
    hour_any, hour_set = _parse_cron_field(parts[1], 0, 23, "hour")
    dom_any, dom_set = _parse_cron_field(parts[2], 1, 31, "day_of_month")
    month_any, month_set = _parse_cron_field(parts[3], 1, 12, "month")
    dow_any, dow_set = _parse_cron_field(parts[4], 0, 7, "day_of_week")

    if 7 in dow_set:
        dow_set.remove(7)
        dow_set.add(0)

    return minute_set, hour_set, dom_set, month_set, dow_set, dom_any, dow_any


def _parse_cron_field(field: str, minimum: int, maximum: int, name: str):
    field = field.strip()
    if not field:
        raise ValueError(f"Empty cron field: {name}")

    if field == "*":
        return True, set(range(minimum, maximum + 1))

    values = set()
    for chunk in field.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise ValueError(f"Invalid empty value in cron field: {name}")

        if "/" in chunk:
            base, step_text = chunk.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise ValueError(f"Invalid step '{step_text}' in cron field: {name}") from exc
            if step <= 0:
                raise ValueError(f"Step must be > 0 in cron field: {name}")
        else:
            base = chunk
            step = 1

        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = _parse_int(start_text, name)
            end = _parse_int(end_text, name)
            if start > end:
                raise ValueError(f"Invalid range '{base}' in cron field: {name}")
        else:
            start = _parse_int(base, name)
            end = start

        if start < minimum or end > maximum:
            raise ValueError(f"Value out of range in cron field {name}: {chunk}")

        for value in range(start, end + 1, step):
            values.add(value)

    if not values:
        raise ValueError(f"No values parsed for cron field: {name}")
    return False, values


def _parse_int(value: str, name: str):
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer '{value}' in cron field: {name}") from exc
