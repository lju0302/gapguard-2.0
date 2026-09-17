"""Dependency-free transport seam for local MQTT-to-Kafka flow tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .processor import process_message


@dataclass
class InMemoryTopic:
    """Ordered append-only topic with Kafka-like zero-based offsets."""

    values: list[dict[str, Any]] = field(default_factory=list)

    def publish(self, value: Mapping[str, Any]) -> int:
        self.values.append(dict(value))
        return len(self.values) - 1


@dataclass
class InMemoryBridge:
    """Model bridge acknowledgement only after local topic append succeeds."""

    topics: dict[str, InMemoryTopic] = field(default_factory=dict)
    _seen_event_ids: set[str] = field(default_factory=set, init=False)

    def publish(self, message: Mapping[str, Any]) -> dict[str, Any]:
        processed = process_message(message)
        topic_name = {
            "telemetry": "gapguard.telemetry.v1",
            "event": "gapguard.alert.v1",
        }[processed["message_type"]]
        topic = self.topics.setdefault(topic_name, InMemoryTopic())
        event_id = processed["message"]["eventId"]
        duplicate = event_id in self._seen_event_ids
        offset = topic.publish(processed["message"])
        self._seen_event_ids.add(event_id)
        return {
            "topic": topic_name,
            "offset": offset,
            "kafka_key": processed["kafka_key"],
            "event_id": event_id,
            "duplicate": duplicate,
        }
