from streaming.common.retry import process_with_retry


def test_retry_returns_after_transient_failure():
    calls = []

    def handler(message):
        calls.append(message["eventId"])
        if len(calls) < 3:
            raise RuntimeError("temporary")

    dlq = []
    result = process_with_retry({"eventId": "run:stream:1"}, handler, dlq.append)

    assert result.status == "processed"
    assert result.attempts == 3
    assert dlq == []


def test_retry_publishes_dlq_after_final_failure():
    dlq = []

    def handler(message):
        raise ValueError("bad payload")

    result = process_with_retry(
        {"eventId": "run:stream:2"},
        handler,
        dlq.append,
        metadata={"originalTopic": "gapguard.alert.v1", "originalOffset": 7},
        max_attempts=2,
    )

    assert result.status == "dlq"
    assert result.attempts == 2
    assert dlq == [
        {
            "eventId": "run:stream:2",
            "retryCount": 2,
            "error": "bad payload",
            "payload": {"eventId": "run:stream:2"},
            "originalTopic": "gapguard.alert.v1",
            "originalOffset": 7,
        }
    ]
