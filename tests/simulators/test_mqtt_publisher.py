import json

from simulators.mqtt_publisher import MQTT_TOPIC, serialize_message


def test_serialize_message_is_compact_utf8_json():
    payload = {"eventId": "run:stream:1", "status": "NORMAL"}

    assert json.loads(serialize_message(payload)) == payload
    assert b" " not in serialize_message(payload)
    assert MQTT_TOPIC == "gapguard/telemetry"
