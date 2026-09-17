import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from edge.edge_monitor.contracts.hashing import (
    attach_data_hash,
    calculate_data_hash,
    verify_data_hash,
)
from edge.edge_monitor.processing import (
    IncompleteWindowError,
    PairedSample,
    SensorSample,
    SensorSummary,
    WindowSummary,
    aggregate_window,
)
from edge.edge_monitor.rules import RuleEngine, Thresholds
from edge.edge_monitor.storage.blackbox import SQLiteBlackbox


class EdgeCorePortTests(unittest.TestCase):
    def test_processing_aggregates_100_samples_and_rejects_incomplete_window(self):
        measured_at = datetime(2026, 9, 15, tzinfo=timezone.utc)
        sample = PairedSample(
            measured_at,
            SensorSample(2.0, -1.0, 3.0, 4.0, 0.0),
            SensorSample(1.0, -0.5, 0.0, 0.0, 0.0),
        )

        summary = aggregate_window([sample] * 100)
        self.assertEqual(summary.sample_count, 100)
        self.assertEqual(summary.top.tilt_x, 2.0)
        self.assertAlmostEqual(summary.top.vibration_rms, 5.0)
        self.assertAlmostEqual(summary.top.peak_acceleration, 5.0)

        with self.assertRaises(IncompleteWindowError) as caught:
            aggregate_window([sample] * 99)
        self.assertEqual(caught.exception.actual_count, 99)

    def test_rules_emit_on_upward_transition(self):
        def summary(relative_tilt_x):
            return WindowSummary(
                datetime(2026, 9, 15, tzinfo=timezone.utc),
                100,
                SensorSummary(relative_tilt_x, 0.0, 0.0, 0.0),
                SensorSummary(0.0, 0.0, 0.0, 0.0),
            )

        engine = RuleEngine(Thresholds(2.0, 5.0, 0.5, 1.0, 2.0, 4.0))
        self.assertEqual(engine.evaluate(summary(1.9)).status, "NORMAL")
        self.assertTrue(engine.evaluate(summary(2.0)).emit_threshold_event)
        self.assertFalse(engine.evaluate(summary(3.0)).emit_threshold_event)
        self.assertEqual(engine.evaluate(summary(5.0)).status, "DANGER")

    def test_hash_is_canonical_and_blackbox_is_fifo_deduplicated_and_durable(self):
        payload = {
            "siteId": "site-a",
            "sectionId": "section-a",
            "deviceId": "edge-1",
            "sourceType": "simulator",
            "measuredAt": "2026-09-15T00:00:01Z",
            "seqNo": 1,
            "details": {"b": 2, "a": 1},
        }
        hashed = attach_data_hash(payload)
        reordered = dict(reversed(list(hashed.items())))
        self.assertEqual(calculate_data_hash(hashed), calculate_data_hash(reordered))
        self.assertTrue(verify_data_hash(hashed))

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "blackbox.db"
            with SQLiteBlackbox(path) as blackbox:
                self.assertEqual(blackbox.next_sequence("edge-1"), 1)
                self.assertEqual(blackbox.next_sequence("edge-1"), 2)
                self.assertTrue(blackbox.enqueue("telemetry", hashed))
                self.assertFalse(blackbox.enqueue("telemetry", hashed))

                second = attach_data_hash({**payload, "seqNo": 2})
                self.assertTrue(blackbox.enqueue("telemetry", second))
                self.assertEqual(
                    [item.payload["seqNo"] for item in blackbox.pending()], [1, 2]
                )
                self.assertTrue(blackbox.quick_check())

            with SQLiteBlackbox(path) as reopened:
                self.assertEqual(reopened.next_sequence("edge-1"), 3)


if __name__ == "__main__":
    unittest.main()
