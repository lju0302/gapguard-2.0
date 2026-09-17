"""Live MQTT-to-Kafka bridge for the local P0 stack.

The transport dependencies are lazy-loaded so contract and in-memory tests stay
dependency-free.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Mapping

from .processor import process_message


KAFKA_TOPICS = {
    "telemetry": "gapguard.telemetry.v1",
    "event": "gapguard.alert.v1",
}
MQTT_TOPICS = tuple(f"gapguard/{kind}" for kind in KAFKA_TOPICS)
LOGGER = logging.getLogger(__name__)


def prepare_publish(message: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Validate one payload and return its Kafka topic, key, and copied value."""

    processed = process_message(message)
    kind = processed["message_type"]
    return KAFKA_TOPICS[kind], str(processed["kafka_key"]), processed["message"]


@dataclass
class LiveBridge:
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    kafka_bootstrap_servers: tuple[str, ...] = ("localhost:9092",)
    mqtt_client_id: str = "gapguard-live-bridge"

    def __post_init__(self) -> None:
        try:
            import paho.mqtt.client as mqtt
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "live bridge dependencies are missing; install "
                "streaming/requirements-transport.txt"
            ) from exc

        if not hasattr(mqtt, "CallbackAPIVersion"):
            raise RuntimeError("live bridge requires paho-mqtt>=2 for manual ACK")
        self._mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.mqtt_client_id,
            manual_ack=True,
        )
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._producer = KafkaProducer(
            bootstrap_servers=list(self.kafka_bootstrap_servers),
            acks="all",
            enable_idempotence=True,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
        )
        self.last_error: str | None = None

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        failed = getattr(reason_code, "is_failure", False)
        if failed:
            self.last_error = f"MQTT connect failed: {reason_code}"
            LOGGER.error(self.last_error)
            return
        for topic in MQTT_TOPICS:
            client.subscribe(topic, qos=1)
        LOGGER.info("MQTT subscribed: %s", ", ".join(MQTT_TOPICS))

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            kafka_topic, key, value = prepare_publish(payload)
            self._producer.send(kafka_topic, key=key, value=value).get(timeout=10)
            client.ack(message.mid, message.qos)
            LOGGER.info("published eventId=%s topic=%s", value["eventId"], kafka_topic)
        except Exception as exc:
            self.last_error = str(exc)
            LOGGER.exception("bridge message failed: %s", self.last_error)
            # Leave the MQTT QoS 1 message unacknowledged and reconnect so the
            # broker can redeliver it after the transient failure is fixed.
            client.disconnect()

    def run_forever(self) -> None:
        self._mqtt.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        self._mqtt.loop_forever()

    def close(self) -> None:
        self._mqtt.disconnect()
        self._producer.flush(timeout=10)
        self._producer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bridge = LiveBridge(
        mqtt_host=os.getenv("GAPGUARD_MQTT_HOST", "localhost"),
        mqtt_port=int(os.getenv("GAPGUARD_MQTT_PORT", "1883")),
        kafka_bootstrap_servers=(
            os.getenv("GAPGUARD_KAFKA_BOOTSTRAP", "localhost:9092"),
        ),
    )
    try:
        bridge.run_forever()
    finally:
        bridge.close()


if __name__ == "__main__":
    main()
