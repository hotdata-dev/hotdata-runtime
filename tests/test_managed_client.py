"""Regression tests for ManagedDatabaseClient result handling."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from hotdata.models.query_response import QueryResponse

import hotdata_framework.managed_client as mc


def _query_response(result_id: str) -> QueryResponse:
    return QueryResponse(
        columns=[],
        rows=[],
        row_count=0,
        preview_row_count=0,
        truncated=False,
        nullable=[],
        result_id=result_id,
        query_run_id="qr",
        execution_time_ms=1,
    )


def test_fetch_table_waits_for_ready_before_arrow(monkeypatch: pytest.MonkeyPatch) -> None:
    """A synchronous ``QueryResponse`` persists its full result out-of-band, and
    that result can still be ``processing`` when the inline preview returns.

    ``fetch_table`` must poll the result to ``ready`` before fetching it as
    Arrow. The earlier bug returned the ``result_id`` immediately on the sync
    path, so Arrow was fetched against a ``processing`` result and failed.
    """
    calls: list[str] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            calls.append("query")
            return _query_response("rslt1")

    statuses = iter(["processing", "processing", "ready"])

    class FakeResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result(self, result_id: str, **kwargs: Any) -> Any:
            status = next(statuses)
            calls.append(f"get_result:{status}")
            return SimpleNamespace(status=status, result_id=result_id, error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            calls.append("arrow")
            return pa.table({"id": [1, 2]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "ResultsApi", FakeResultsApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", FakeArrowResultsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    table = client.fetch_table(database="mydb", schema="public", table="orders")

    assert table is not None
    assert table.num_rows == 2
    # The result was polled to readiness, and Arrow was fetched only afterwards.
    assert "get_result:processing" in calls
    assert "get_result:ready" in calls
    assert calls.index("arrow") > calls.index("get_result:ready")


def _fake_runtime() -> SimpleNamespace:
    return SimpleNamespace(
        api=object(),
        resolve_managed_database=lambda name: SimpleNamespace(id="db1", default_connection_id="c"),
        list_managed_tables=lambda database, schema=None: [
            SimpleNamespace(table="orders", synced=True)
        ],
    )


def test_fetch_table_carries_database_scope_on_result_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Results (and runs) of a database-scoped query are database-scoped:
    the results endpoints 400 with "X-Database-Id header is required" when
    the scope is missing. ``fetch_table`` must carry the database id on the
    result poll and the Arrow fetch, not only on the query submit — the
    hotdata 0.6.0 SDK exposes ``x_database_id`` on all three.

    Regression: reruns/append loads against an existing synced table failed
    with an opaque ``400: Bad Request`` because both reads omitted the scope.
    """
    result_scopes: list[str | None] = []
    arrow_scopes: list[str | None] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            assert x_database_id == "db1"
            return _query_response("rslt1")

    class FakeResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result(self, result_id: str, *, x_database_id: str | None = None) -> Any:
            result_scopes.append(x_database_id)
            return SimpleNamespace(status="ready", result_id=result_id, error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        # x_database_id is REQUIRED in the 0.6.0 SDK — mirroring that here
        # makes this test fail if a caller ever drops the scope again.
        def get_result_arrow(self, result_id: str, *, x_database_id: str) -> pa.Table:
            arrow_scopes.append(x_database_id)
            return pa.table({"id": [1]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "ResultsApi", FakeResultsApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", FakeArrowResultsApi)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    table = client.fetch_table(database="mydb", schema="public", table="orders")

    assert table is not None
    assert result_scopes == ["db1"]
    assert arrow_scopes == ["db1"]


def _load_recording_runtime(
    calls: list[str], uploads: list[str] | None = None
) -> SimpleNamespace:
    """A runtime whose ``load_managed_table`` records each mode and always fails
    with a transient error, so retry behaviour is observable via ``calls``.

    ``uploads`` records the upload id each attempt was sent with — the invariant
    that makes retrying safe, so it is worth being able to assert on."""

    def load_managed_table(
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
        mode: str,
        key: list[str] | None = None,
    ) -> SimpleNamespace:
        calls.append(mode)
        if uploads is not None:
            uploads.append(upload_id)
        raise TimeoutError("commit succeeded but response was lost")

    runtime = _fake_runtime()
    runtime.load_managed_table = load_managed_table
    return runtime


def _managed_client(max_retries: int) -> Any:
    return mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=max_retries,
        retry_backoff_seconds=0.0,
    )


def test_append_load_retries_like_every_other_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """``append`` is retried, because the retry re-sends the same ``upload_id``
    and the server replays its receipt for that id instead of appending twice.

    The mode was never what made a retry unsafe, so excluding ``append`` bought
    no safety and cost it the whole retry budget — which is the budget that
    outlasts a table's write lock."""
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)
    calls: list[str] = []
    client = _managed_client(max_retries=3)
    client._runtime = _load_recording_runtime(calls)

    with pytest.raises(mc.HotdataTransientError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="append")

    assert calls == ["append", "append", "append"]


def test_a_retried_load_re_sends_the_same_upload_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """The upload id is the invariant the safety of a retried append rests on:
    the server keys its replay receipt on it, so a retry that re-staged the
    upload would mint a new id, find no receipt, and duplicate the rows.

    Staging happens in ``upload_parquet``, outside the retried operation. This
    pins that arrangement, which a refactor moving the upload inward would
    silently break."""
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)
    calls: list[str] = []
    uploads: list[str] = []
    client = _managed_client(max_retries=4)
    client._runtime = _load_recording_runtime(calls, uploads)

    with pytest.raises(mc.HotdataTransientError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="append")

    assert uploads == ["u1", "u1", "u1", "u1"]


def test_idempotent_load_retries_on_transient(monkeypatch: pytest.MonkeyPatch) -> None:
    """Idempotent modes still exhaust the retry budget on transient errors."""
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)
    calls: list[str] = []
    client = _managed_client(max_retries=3)
    client._runtime = _load_recording_runtime(calls)

    with pytest.raises(mc.HotdataTransientError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="replace")

    assert calls == ["replace", "replace", "replace"]  # retried up to max_retries


def test_retry_delay_floors_on_the_servers_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Retry-After`` states how long the refused condition lasts; the ramp only
    knows how many attempts are left. Taking the larger of the two respects both,
    so an early attempt does not retry in 1.5s against a lock the server just
    said would hold for 5."""
    monkeypatch.setattr(mc.random, "random", lambda: 0.0)
    client = _managed_client(max_retries=20)
    client._retry_backoff_seconds = 1.5

    assert client._retry_delay(attempt=1, retry_after_seconds=5.0) == 5.0
    # Past the point the ramp overtakes it, the ramp wins and the floor is inert.
    assert client._retry_delay(attempt=10, retry_after_seconds=5.0) == 15.0


def test_retry_delay_only_ever_adds_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Jitter is added, never subtracted, so a stated ``Retry-After`` is never
    undercut — the point of spreading is to stop writers colliding, not to
    retry sooner than the server asked."""
    client = _managed_client(max_retries=20)
    client._retry_backoff_seconds = 1.5

    monkeypatch.setattr(mc.random, "random", lambda: 1.0)
    assert client._retry_delay(attempt=1, retry_after_seconds=5.0) == pytest.approx(7.5)
    monkeypatch.setattr(mc.random, "random", lambda: 0.0)
    assert client._retry_delay(attempt=1, retry_after_seconds=5.0) == 5.0


def test_retry_delay_caps_the_ramp_and_the_floor_but_not_the_jitter() -> None:
    """The cap bounds what the ramp and a server-stated floor can ask for. The
    jitter deliberately sits above it: clamping the total would land every late
    attempt on exactly _MAX_BACKOFF_SECONDS and re-correlate the waits that most
    need spreading."""
    client = _managed_client(max_retries=20)
    client._retry_backoff_seconds = 1.5
    cap = mc.ManagedDatabaseClient._MAX_BACKOFF_SECONDS
    ceiling = cap * (1.0 + mc.ManagedDatabaseClient._RETRY_JITTER_FRACTION)

    # attempt 100 would ramp to 150s, and an hour-long Retry-After is refused too.
    assert cap <= client._retry_delay(attempt=100, retry_after_seconds=None) <= ceiling
    assert cap <= client._retry_delay(attempt=1, retry_after_seconds=3600.0) <= ceiling


def test_retry_delay_decorrelates_callers_that_started_together() -> None:
    """Writers refused by one table's lock started together, so an unjittered
    ramp has them re-collide on every attempt. Identical inputs must not produce
    an identical wait."""
    client = _managed_client(max_retries=20)
    client._retry_backoff_seconds = 1.5

    delays = {client._retry_delay(attempt=3, retry_after_seconds=5.0) for _ in range(50)}

    assert len(delays) > 1


def test_load_managed_table_forwards_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """A per-load ``key`` is passed straight through to the runtime client."""
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)
    captured: dict[str, Any] = {}

    def load_managed_table(
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
        mode: str,
        key: list[str] | None = None,
    ) -> SimpleNamespace:
        captured["mode"] = mode
        captured["key"] = key
        return SimpleNamespace(
            connection_id="c", schema_name=schema, table_name=table, row_count=0
        )

    client = _managed_client(max_retries=1)
    runtime = _fake_runtime()
    runtime.load_managed_table = load_managed_table
    client._runtime = runtime

    client.load_managed_table(
        "db", "orders", schema="public", upload_id="u1", mode="delete", key=["id"]
    )
    assert captured == {"mode": "delete", "key": ["id"]}
