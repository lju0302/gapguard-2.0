import copy
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from edge.edge_monitor.contracts.hashing import verify_data_hash
from edge.edge_monitor.contracts.message_builder import (
    build_event_message,
    build_telemetry_message,
)
from edge.edge_monitor.processing import SensorSummary, WindowSummary


class MessageBuilderTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(
            site_id="site-001",
            section_id="section-001",
            device_id="edge-001",
            source_type="simulator",
        )
        self.measured_at = datetime(2026, 9, 15, 0, 0, 1, tzinfo=timezone.utc)
        self.produced_at = datetime(2026, 9, 15, 0, 0, 1, 5000, tzinfo=timezone.utc)
        self.summary = WindowSummary(
            self.measured_at,
            100,
            SensorSummary(1.2, -0.4, 0.18, 0.42),
            SensorSummary(0.8, -0.1, 0.16, 0.37),
        )

    def test_telemetry_fields_relative_tilt_and_hash(self):
        message = build_telemetry_message(
            self.config,
            self.summary,
            1842,
            "run-001",
            "stream-001",
            self.produced_at,
            status="WARNING",
        )

        self.assertEqual(
            {
                key: message[key]
                for key in (
                    "siteId",
                    "sectionId",
                    "deviceId",
                    "sourceType",
                    "seqNo",
                    "eventId",
                    "streamId",
                    "schemaVersion",
                    "testRunId",
                )
            },
            {
                "siteId": "site-001",
                "sectionId": "section-001",
                "deviceId": "edge-001",
                "sourceType": "simulator",
                "seqNo": 1842,
                "eventId": "run-001:stream-001:1842",
                "streamId": "stream-001",
                "schemaVersion": 3,
                "testRunId": "run-001",
            },
        )
        self.assertEqual(message["measuredAt"], "2026-09-15T00:00:01Z")
        self.assertEqual(message["producedAt"], "2026-09-15T00:00:01.005000Z")
        self.assertEqual(message["sensors"]["top"]["i2cAddress"], "0x69")
        self.assertEqual(message["sensors"]["bottom"]["i2cAddress"], "0x68")
        self.assertAlmostEqual(message["relativeTiltX"], 0.4)
        self.assertAlmostEqual(message["relativeTiltY"], -0.3)
        self.assertEqual(message["status"], "WARNING")
        self.assertTrue(verify_data_hash(message))

    def test_event_fields_default_details_and_hash(self):
        details = {"relatedSeqNo": 1842}
        message = build_event_message(
            self.config,
            self.measured_at,
            1843,
            "run-001",
            "stream-001",
            "THRESHOLD_EXCEEDED",
            "DANGER",
            "relative tilt threshold exceeded",
            details,
            self.produced_at,
        )

        self.assertEqual(message["eventId"], "run-001:stream-001:1843")
        self.assertEqual(message["eventType"], "THRESHOLD_EXCEEDED")
        self.assertEqual(message["severity"], "DANGER")
        self.assertEqual(message["details"], details)
        self.assertTrue(verify_data_hash(message))
        self.assertEqual(
            build_event_message(
                self.config,
                self.measured_at,
                1844,
                "run-001",
                "stream-001",
                "DEVICE_STARTED",
                "INFO",
                "device started",
            )["details"],
            {},
        )

    def test_builders_do_not_mutate_inputs(self):
        details = {"nested": {"value": 1}}
        original_summary = copy.deepcopy(self.summary)
        original_details = copy.deepcopy(details)

        build_telemetry_message(
            self.config, self.summary, 1, "run", "stream", self.produced_at
        )
        build_event_message(
            self.config,
            self.measured_at,
            2,
            "run",
            "stream",
            "SENSOR_ERROR",
            "ERROR",
            "sensor read failed",
            details,
            self.produced_at,
        )

        self.assertEqual(self.summary, original_summary)
        self.assertEqual(details, original_details)

    def test_invalid_event_type_and_severity_are_rejected(self):
        with self.assertRaises(ValueError):
            build_event_message(
                self.config,
                self.measured_at,
                1,
                "run",
                "stream",
                "UNKNOWN_EVENT",
                "INFO",
                "reason",
            )
        with self.assertRaises(ValueError):
            build_event_message(
                self.config,
                self.measured_at,
                1,
                "run",
                "stream",
                "DEVICE_STARTED",
                "UNKNOWN",
                "reason",
            )


if __name__ == "__main__":
    unittest.main()
