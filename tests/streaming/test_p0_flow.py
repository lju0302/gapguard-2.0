from datetime import datetime, timezone

from streaming.p0_flow import run_local_p0


def test_local_p0_runs_all_runtimes_through_storage():
    result = run_local_p0(
        "run-001",
        measured_at=datetime(2026, 9, 15, tzinfo=timezone.utc),
        produced_at=datetime(2026, 9, 15, 0, 0, 0, 1000, tzinfo=timezone.utc),
    )

    assert result.runtime_count == 8
    assert result.published_count == 8
    assert result.stored_count == 8
    assert result.duplicate_count == 0
    assert result.next_offset == 8
