import json
import hashlib
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]


def _load_schema(name):
    return json.loads((ROOT / "contracts" / name).read_text())


def _load_example(name):
    return json.loads((ROOT / "contracts" / "examples" / name).read_text())


def _validator(schema_name):
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load_schema(schema_name)
    return jsonschema.Draft202012Validator(schema)


def test_v3_examples_validate():
    for schema_name, example_name in (
        ("edge-telemetry-v3.schema.json", "edge-telemetry-v3.json"),
        ("edge-event-v3.schema.json", "edge-event-v3.json"),
    ):
        payload = _load_example(example_name)
        _validator(schema_name).validate(payload)
        unsigned = {key: value for key, value in payload.items() if key != "dataHash"}
        digest = hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        assert payload["dataHash"] == digest


@pytest.mark.parametrize("field", ["eventId", "streamId", "schemaVersion", "producedAt", "testRunId"])
def test_v3_common_fields_are_required(field):
    payload = _load_example("edge-telemetry-v3.json")
    payload.pop(field)
    with pytest.raises(Exception):
        _validator("edge-telemetry-v3.schema.json").validate(payload)


def test_v3_rejects_wrong_version_and_sensor_address():
    payload = _load_example("edge-telemetry-v3.json")
    payload["schemaVersion"] = 2
    with pytest.raises(Exception):
        _validator("edge-telemetry-v3.schema.json").validate(payload)

    payload = _load_example("edge-telemetry-v3.json")
    payload["sensors"]["top"]["i2cAddress"] = "0x68"
    with pytest.raises(Exception):
        _validator("edge-telemetry-v3.schema.json").validate(payload)


def test_v3_event_enum_is_closed():
    payload = _load_example("edge-event-v3.json")
    payload["eventType"] = "UNKNOWN_EVENT"
    with pytest.raises(Exception):
        _validator("edge-event-v3.schema.json").validate(payload)
