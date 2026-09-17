"""Kafka publisher for messages that exhausted local retry attempts."""

from __future__ import annotations

import json
from typing import Any, Mapping


def build_dlq_metadata(
    topic: str,
    partition: int,
    offset: int,
    event_id: str | None,
    retry_count: int,
    error: str,
) -> dict[str, Any]:
    if not topic or partition < 0 or offset < 0 or retry_count <= 0:
        raise ValueError("invalid DLQ metadata")
    return {
        "originalTopic": topic,
        "originalPartition": partition,
        "originalOffset": offset,
        "eventId": event_id,
        "retryCount": retry_count,
        "error": error[:1000],
    }


class KafkaDlqPublisher:
    def __init__(
        self,
        bootstrap_servers: str | list[str] = "localhost:9092",
        topic: str = "gapguard.dlq.v1",
    ) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError("install streaming/requirements-transport.txt") from exc
        servers = [bootstrap_servers] if isinstance(bootstrap_servers, str) else bootstrap_servers
        self._producer = KafkaProducer(
            bootstrap_servers=servers,
            acks="all",
            enable_idempotence=True,
            key_serializer=lambda value: str(value or "unknown").encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
        )
        self.topic = topic

    def publish(self, record: Mapping[str, Any]) -> None:
        event_id = record.get("eventId")
        self._producer.send(self.topic, key=event_id, value=dict(record)).get(timeout=10)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close()
