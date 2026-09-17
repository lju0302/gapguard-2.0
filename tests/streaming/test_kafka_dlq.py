import pytest

from streaming.consumers.kafka_dlq import build_dlq_metadata


def test_build_dlq_metadata_preserves_source_position_and_bounds_error():
    result = build_dlq_metadata("gapguard.alert.v1", 2, 17, "run:stream:1", 3, "x" * 1200)

    assert result["originalTopic"] == "gapguard.alert.v1"
    assert result["originalPartition"] == 2
    assert result["originalOffset"] == 17
    assert result["eventId"] == "run:stream:1"
    assert result["retryCount"] == 3
    assert len(result["error"]) == 1000


def test_build_dlq_metadata_rejects_invalid_position():
    with pytest.raises(ValueError, match="invalid DLQ metadata"):
        build_dlq_metadata("gapguard.alert.v1", -1, 17, None, 3, "bad")
