from __future__ import annotations

import json
import urllib.error
import urllib.request


def send_slack_notification(webhook_url: str | None, message: str, timeout: int = 10) -> bool:
    if not webhook_url:
        return False

    payload = json.dumps({"text": message}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 300
    except urllib.error.URLError:
        return False
