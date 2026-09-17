from datetime import datetime, timezone

from edge.edge_monitor.config import load_config
from simulators.fault_scenarios import connection_loss_scenario
from simulators.vm_simulator import default_runtimes
from streaming.common.message import verify_data_hash


def test_connection_loss_scenario_keeps_event_order_and_hashes():
    runtime = default_runtimes("fault-run")[0]
    lost_at = datetime(2026, 9, 16, tzinfo=timezone.utc)
    recovered_at = datetime(2026, 9, 16, 0, 0, 5, tzinfo=timezone.utc)

    lost, recovered = connection_loss_scenario(runtime, load_config(), lost_at, recovered_at)

    assert lost["eventType"] == "CONNECTION_LOST"
    assert recovered["eventType"] == "CONNECTION_RECOVERED"
    assert lost["seqNo"] == 1
    assert recovered["seqNo"] == 2
    assert lost["eventId"] == "fault-run:site-001-wall-001:1"
    assert recovered["eventId"] == "fault-run:site-001-wall-001:2"
    assert verify_data_hash(lost)
    assert verify_data_hash(recovered)
