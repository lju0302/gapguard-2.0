"""Small retry and DLQ policy shared by local consumers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class RetryResult:
    status: Literal["processed", "dlq"]
    attempts: int
    error: str | None = None


def process_with_retry(
    message: Mapping[str, Any],
    handler: Callable[[Mapping[str, Any]], None],
    publish_dlq: Callable[[dict[str, Any]], None],
    metadata: Mapping[str, Any] | None = None,
    max_attempts: int = 3,
) -> RetryResult:
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    last_error = "unknown processing error"
    for attempt in range(1, max_attempts + 1):
        try:
            handler(message)
            return RetryResult("processed", attempt)
        except Exception as exc:  # consumer boundary must convert failures to DLQ
            last_error = str(exc) or exc.__class__.__name__

    dlq = {
        "eventId": message.get("eventId"),
        "retryCount": max_attempts,
        "error": last_error[:1000],
        "payload": dict(message),
    }
    if metadata:
        dlq.update(dict(metadata))
    dlq["error"] = last_error[:1000]
    publish_dlq(dlq)
    return RetryResult("dlq", max_attempts, last_error)
