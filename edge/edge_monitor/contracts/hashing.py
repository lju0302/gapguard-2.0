"""Canonical SHA-256 helpers for Edge payloads."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def canonical_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize a payload deterministically, excluding its hash field."""

    hash_input = {key: value for key, value in payload.items() if key != "dataHash"}
    return json.dumps(
        hash_input,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def calculate_data_hash(payload: Mapping[str, Any]) -> str:
    """Return the lowercase SHA-256 hex digest for a payload."""

    return hashlib.sha256(canonical_payload_bytes(payload)).hexdigest()


def attach_data_hash(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a payload copy with a freshly calculated ``dataHash``."""

    result = dict(payload)
    result["dataHash"] = calculate_data_hash(result)
    return result


def verify_data_hash(payload: Mapping[str, Any]) -> bool:
    """Validate the payload's hash without mutating it."""

    value = payload.get("dataHash")
    return isinstance(value, str) and value == calculate_data_hash(payload)
