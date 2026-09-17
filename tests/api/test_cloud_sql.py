from api.cloud_sql import CloudSqlQueryService


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, query, params=()):
        self.connection.queries.append((query, params))
        if query.startswith("SELECT COUNT(*)"):
            self.rows = [(self.connection.counts[query.rsplit(" ", 1)[-1]],)]
        else:
            self.rows = self.connection.payload_rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]

    def close(self):
        pass


class FakeConnection:
    payload_rows = [{"payload_json": {"eventId": "run:stream:1"}}]
    counts = {"telemetry": 1, "alerts": 2}

    def __init__(self):
        self.queries = []

    def cursor(self):
        return FakeCursor(self)


def test_cloud_sql_query_service_uses_parameterized_read_queries():
    connection = FakeConnection()
    service = CloudSqlQueryService(connection)

    assert service.list_telemetry(site_id="site-001", limit=10) == [
        {"eventId": "run:stream:1"}
    ]
    assert service.list_alerts() == [{"eventId": "run:stream:1"}]
    assert service.summary() == {"telemetry_count": 1, "alert_count": 2}
    assert connection.queries[0][1] == ("site-001", 10)
    assert "%s" in connection.queries[0][0]


def test_cloud_sql_query_service_rejects_invalid_limits():
    service = CloudSqlQueryService(FakeConnection())
    try:
        service.list_alerts(limit=0)
    except ValueError as exc:
        assert "between 1 and 1000" in str(exc)
    else:
        raise AssertionError("invalid limit was accepted")
