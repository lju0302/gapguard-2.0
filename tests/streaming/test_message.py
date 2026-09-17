import unittest

from streaming.common.message import (
    canonical_json_without_hash,
    compute_data_hash,
    kafka_key,
    message_type,
    validate_identity,
    verify_data_hash,
)


class MessageTest(unittest.TestCase):
    def setUp(self):
        self.message = {
            "zeta": "한글",
            "streamId": "site-001-wall-001",
            "testRunId": "run-001",
            "seqNo": 1842,
            "schemaVersion": 3,
            "eventId": "run-001:site-001-wall-001:1842",
        }

    def test_hash_round_trip_and_canonical_bytes(self):
        original = dict(self.message)
        self.message["dataHash"] = compute_data_hash(self.message)

        self.assertTrue(verify_data_hash(self.message))
        self.assertEqual(
            canonical_json_without_hash(self.message),
            b'{"eventId":"run-001:site-001-wall-001:1842","schemaVersion":3,"seqNo":1842,"streamId":"site-001-wall-001","testRunId":"run-001","zeta":"\xed\x95\x9c\xea\xb8\x80"}',
        )
        self.assertEqual(original, {key: value for key, value in self.message.items() if key != "dataHash"})

    def test_hash_mismatch(self):
        self.message["dataHash"] = compute_data_hash(self.message)
        self.message["seqNo"] = 1843

        self.assertFalse(verify_data_hash(self.message))

    def test_validate_identity(self):
        validate_identity(self.message)

        for field, value in (
            ("eventId", "run-001:other-stream:1842"),
            ("schemaVersion", 2),
            ("seqNo", -1),
        ):
            invalid = dict(self.message)
            invalid[field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    validate_identity(invalid)

    def test_validate_identity_missing_field(self):
        invalid = dict(self.message)
        del invalid["eventId"]

        with self.assertRaises(ValueError):
            validate_identity(invalid)

    def test_message_type(self):
        self.assertEqual(message_type(self.message), "telemetry")
        event = dict(self.message, eventType="THRESHOLD_EXCEEDED")
        self.assertEqual(message_type(event), "event")

    def test_kafka_key(self):
        self.assertEqual(kafka_key(self.message), "site-001-wall-001")


if __name__ == "__main__":
    unittest.main()
