"""MQTT publisher for the eight local simulator runtimes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping


MQTT_TOPIC = "gapguard/telemetry"


def serialize_message(message: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(message), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


class MqttSimulatorPublisher:
    def __init__(self, host: str = "localhost", port: int = 1883, client_id: str = "gapguard-simulator") -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("install streaming/requirements-transport.txt") from exc
        self._mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
        self._host = host
        self._port = port

    def connect(self) -> None:
        self._mqtt.loop_start()
        self._mqtt.connect(self._host, self._port, keepalive=60)

    def publish_one(self, message: Mapping[str, Any]) -> None:
        result = self._mqtt.publish(MQTT_TOPIC, serialize_message(message), qos=1)
        result.wait_for_publish()
        if not result.is_published():
            raise RuntimeError("MQTT publish was not acknowledged")

    def loop_start(self) -> None:
        self._mqtt.loop_start()

    def loop_stop(self) -> None:
        self._mqtt.loop_stop()

    def close(self) -> None:
        self._mqtt.disconnect()
        self._mqtt.loop_stop()


def publish_default_runtimes(test_run_id: str) -> int:
    from edge.edge_monitor.config import load_config
    from simulators.vm_simulator import default_runtimes, run_once

    now = datetime.now(timezone.utc)
    config = load_config()
    publisher = MqttSimulatorPublisher(client_id=f"gapguard-{test_run_id}")
    publisher.connect()
    try:
        runtimes = default_runtimes(test_run_id)
        for runtime in runtimes:
            publisher.publish_one(run_once(runtime, config, 1, now, now))
        return len(runtimes)
    finally:
        publisher.close()


if __name__ == "__main__":
    print(json.dumps({"published_count": publish_default_runtimes("local-run")}))
