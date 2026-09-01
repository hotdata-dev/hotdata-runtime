"""Regression tests for ManagedDatabaseClient result handling."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from hotdata.arrow import ResultNotReadyError
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_response import QueryResponse
from hotdata.rest import ApiException

import hotdata_framework.managed_client as mc
from hotdata_framework.errors import HotdataTerminalError, HotdataTransientError


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


def _async_query_response() -> AsyncQueryResponse:
    return AsyncQueryResponse(
        query_run_id="qr",
        status="running",
        status_url="/v1/query-runs/qr",
    )


def test_fetch_table_waits_on_the_query_run_not_the_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness is the query run's answer to give, not the result endpoint's.

    A synchronous ``QueryResponse`` returns its rows inline but goes on saving
    the full result in the background, so the reply is not the finish line and
    Arrow cannot be fetched yet. The run turns ``succeeded`` only once that
    result is saved and ready, which makes it a sufficient readiness signal on
    its own.

    The earlier version polled ``GET /results/{id}`` instead, which is the
    result *data* endpoint: a ready JSON reply carries the whole result, so the
    check downloaded all of it to read one status field, and the server refuses
    that outright once the result is large enough.
    """
    calls: list[str] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            calls.append("query")
            return _query_response("rslt1")

    statuses = iter(["running", "running", "succeeded"])

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            status = next(statuses)
            calls.append(f"get_query_run:{status}")
            return SimpleNamespace(status=status, result_id="rslt1", error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            calls.append("arrow")
            return pa.table({"id": [1, 2]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
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
    # The run was polled while still running, and Arrow was fetched only once
    # it had succeeded.
    assert "get_query_run:running" in calls
    assert calls.index("arrow") > calls.index("get_query_run:succeeded")


def test_readiness_never_touches_the_json_results_endpoint() -> None:
    """The generated ``ResultsApi`` -- the JSON result *data* endpoint -- must
    not be reachable from this module at all.

    Its absence is the fix: readiness comes from the query run, and the only
    result endpoint this client is allowed to call is the streaming Arrow one,
    imported under a distinct name. A future edit that reintroduces the plain
    ``ResultsApi`` here is reintroducing a whole-result download per poll.
    """
    assert not hasattr(mc, "ResultsApi")
    assert hasattr(mc, "ArrowResultsApi")


def _fake_runtime() -> SimpleNamespace:
    return SimpleNamespace(
        api=object(),
        resolve_managed_database=lambda name: SimpleNamespace(id="db1", default_connection_id="c"),
        list_managed_tables=lambda database, schema=None: [
            SimpleNamespace(table="orders", synced=True)
        ],
    )


def test_fetch_table_carries_database_scope_on_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runs (and results) of a database-scoped query are database-scoped: the
    endpoints 400 with "X-Database-Id header is required" when the scope is
    missing. ``fetch_table`` must carry the database id on the run poll and the
    Arrow fetch, not only on the query submit.

    Regression: reruns/append loads against an existing synced table failed
    with an opaque ``400: Bad Request`` because both reads omitted the scope.
    """
    run_scopes: list[str | None] = []
    arrow_scopes: list[str | None] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            assert x_database_id == "db1"
            return _query_response("rslt1")

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        # x_database_id is REQUIRED on this endpoint -- mirroring that here
        # makes this test fail if a caller ever drops the scope again.
        def get_query_run(self, query_run_id: str, *, x_database_id: str) -> Any:
            run_scopes.append(x_database_id)
            return SimpleNamespace(status="succeeded", result_id="rslt1", error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, *, x_database_id: str) -> pa.Table:
            arrow_scopes.append(x_database_id)
            return pa.table({"id": [1]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
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
    assert run_scopes == ["db1"]
    assert arrow_scopes == ["db1"]


def test_interrupted_query_run_is_retried_not_waited_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``interrupted`` is terminal but safe to retry: the server lost the run
    rather than rejecting the query.

    It has to be raised as transient so the surrounding retry re-runs the
    query. The earlier poll recognised only ``failed`` and a ``cancelled``
    status the API never sends, so an interrupted run matched neither -- it
    spun for the full five-minute timeout and then failed.
    """
    calls: list[str] = []
    statuses = iter(["interrupted", "succeeded"])

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            calls.append("query")
            return _query_response("rslt1")

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            status = next(statuses)
            calls.append(f"get_query_run:{status}")
            return SimpleNamespace(
                status=status,
                result_id="rslt1",
                error_message="instance lost" if status == "interrupted" else None,
            )

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            calls.append("arrow")
            return pa.table({"id": [1]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", FakeArrowResultsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=2,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    table = client.fetch_table(database="mydb", schema="public", table="orders")

    assert table is not None
    # The interrupted run re-ran the query rather than being waited out.
    assert calls.count("query") == 2
    assert calls == [
        "query",
        "get_query_run:interrupted",
        "query",
        "get_query_run:succeeded",
        "arrow",
    ]


def test_failed_query_run_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """``failed`` means the query itself failed, so every retry reaches the same
    answer. It must surface the run's own message rather than being re-run."""
    calls: list[str] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            calls.append("query")
            return _query_response("rslt1")

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            return SimpleNamespace(
                status="failed", result_id=None, error_message="no such column: nope"
            )

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=3,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    with pytest.raises(HotdataTerminalError, match="no such column"):
        client.fetch_table(database="mydb", schema="public", table="orders")

    assert calls.count("query") == 1


def test_a_succeeded_run_that_saved_no_result_raises_rather_than_reading_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one state where an empty answer would be a wrong answer.

    A run succeeds with no `result_id` when its rows came back inline but the
    result could not be saved. Answering `None` puts that on the same footing as
    a table that does not exist -- `fetch_table_rows` turns both into `[]` -- so
    a read-modify-write load would read no existing rows and write only its new
    batch, dropping the rows already in the table. Silent data loss is the worst
    outcome available here, so this raises, and raises terminally: re-running
    cannot save a result that was already discarded.

    The query reply carries an id of its own, and it must not be believed over
    the run's.
    """
    arrow_calls: list[str] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            # The reply hands out an id...
            return _query_response("rslt1")

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            # ...that the run says was never saved.
            return SimpleNamespace(
                status="succeeded",
                result_id=None,
                error_message=None,
                warning_message="result row creation failed; result not persisted",
            )

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            arrow_calls.append(result_id)
            return pa.table({"id": [1]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", FakeArrowResultsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=3,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    with pytest.raises(HotdataTerminalError, match="result was not saved"):
        client.fetch_table(database="mydb", schema="public", table="orders")

    # The reply's id was never used.
    assert arrow_calls == []


def test_fetch_table_rows_cannot_turn_an_unsaved_result_into_no_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`fetch_table_rows` is where an empty answer does the damage.

    It maps `None` to `[]`, which is also its answer for a table that is not
    synced, so the unsaved-result case must not be able to reach that mapping.
    """

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> QueryResponse:
            return _query_response("rslt1")

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            return SimpleNamespace(
                status="succeeded", result_id=None, error_message=None, warning_message=None
            )

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()

    with pytest.raises(HotdataTerminalError):
        client.fetch_table_rows(database="mydb", schema="public", table="orders")


def _load_recording_runtime(calls: list[str], uploads: list[str] | None = None) -> SimpleNamespace:
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


def _lock_refusing_runtime(retry_after: str | None) -> SimpleNamespace:
    """A runtime whose loads are refused the way the API refuses a contended
    table: ``409 RESOURCE_LOCKED``, optionally carrying ``Retry-After``."""

    def load_managed_table(
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
        mode: str,
        key: list[str] | None = None,
    ) -> SimpleNamespace:
        error = ApiException(
            status=409,
            reason="Conflict",
            body='{"error":{"code":"RESOURCE_LOCKED","message":"retry shortly"}}',
        )
        error.headers = {"Retry-After": retry_after} if retry_after else {}
        raise error

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


def test_a_lock_refusal_is_retried_and_waits_the_header_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end for the contended-table case: a `409 RESOURCE_LOCKED` is
    classified transient, retried, and each wait honours the `Retry-After` the
    refusal carried.

    Worth having as one test rather than three: `_retry_delay` is exercised
    directly elsewhere, but nothing else covers the wiring that carries the
    header off the error and into the sleep. The other retry tests stub sleep
    with a lambda that discards its argument, so a regression passing `None`
    here — or swapping the two positional arguments — would leave them green."""
    slept: list[float] = []
    monkeypatch.setattr(mc.time, "sleep", lambda seconds: slept.append(seconds))
    monkeypatch.setattr(mc.random, "random", lambda: 0.0)
    client = _managed_client(max_retries=4)
    client._retry_backoff_seconds = 1.5
    client._runtime = _lock_refusing_runtime(retry_after="5")

    with pytest.raises(mc.HotdataTransientError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="append")

    # Four attempts, so three waits — each floored on the header rather than
    # taking the ramp's 1.5s / 3.0s / 4.5s.
    assert slept == [5.0, 5.0, 5.0]


def test_a_lock_refusal_without_a_header_falls_back_to_the_ramp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The floor is the header's contribution, not a hard-coded one: with no
    header the ramp decides, which keeps the refusal path working against a
    server that does not state a wait."""
    slept: list[float] = []
    monkeypatch.setattr(mc.time, "sleep", lambda seconds: slept.append(seconds))
    monkeypatch.setattr(mc.random, "random", lambda: 0.0)
    client = _managed_client(max_retries=4)
    client._retry_backoff_seconds = 1.5
    client._runtime = _lock_refusing_runtime(retry_after=None)

    with pytest.raises(mc.HotdataTransientError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="append")

    assert slept == [1.5, 3.0, 4.5]


def test_a_permanent_conflict_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A `CONFLICT` cannot succeed as posted, so it surfaces on the first
    attempt instead of spending the budget to reach the same 409."""
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)
    attempts: list[int] = []

    def load_managed_table(*_args: object, **_kwargs: object) -> SimpleNamespace:
        attempts.append(1)
        error = ApiException(
            status=409,
            reason="Conflict",
            body='{"error":{"code":"CONFLICT","message":"upload already consumed"}}',
        )
        error.headers = {}
        raise error

    client = _managed_client(max_retries=8)
    client._runtime = _fake_runtime()
    client._runtime.load_managed_table = load_managed_table

    with pytest.raises(HotdataTerminalError):
        client.load_managed_table("db", "orders", schema="public", upload_id="u1", mode="append")

    assert len(attempts) == 1


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
        return SimpleNamespace(connection_id="c", schema_name=schema, table_name=table, row_count=0)

    client = _managed_client(max_retries=1)
    runtime = _fake_runtime()
    runtime.load_managed_table = load_managed_table
    client._runtime = runtime

    client.load_managed_table(
        "db", "orders", schema="public", upload_id="u1", mode="delete", key=["id"]
    )
    assert captured == {"mode": "delete", "key": ["id"]}


def test_query_is_submitted_async_so_no_preview_is_built(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fetch_table`` wants a result id, not rows.

    A synchronous submit always serialises an inline preview of the result into
    its reply, and there is no request field that suppresses it -- so the only
    way not to be sent megabytes this path never reads is to ask
    asynchronously. The async reply carries a run id and nothing else.
    """
    requests: list[Any] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: Any, *, x_database_id: str) -> AsyncQueryResponse:
            requests.append(request)
            return _async_query_response()

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            return SimpleNamespace(status="succeeded", result_id="rslt1", error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            return pa.table({"id": [1]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
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
    assert len(requests) == 1
    # The attribute alone is not the contract: the field only reaches the server
    # as `async`. Were the alias missing or misspelled, the attribute assertion
    # would still pass, the server would ignore the field and answer
    # synchronously with a full preview -- restoring the exact behaviour this
    # change removes, silently.
    assert requests[0].to_dict()["async"] is True


def test_async_reply_is_followed_through_to_arrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The async reply carries no ``result_id`` at all -- only a run id.

    So the run is not merely the cheapest way to learn the result is ready, it
    is the only way to learn the result's id in the first place.
    """
    calls: list[str] = []
    statuses = iter(["running", "succeeded"])

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> AsyncQueryResponse:
            calls.append("query")
            return _async_query_response()

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            assert query_run_id == "qr"
            status = next(statuses)
            calls.append(f"get_query_run:{status}")
            return SimpleNamespace(
                status=status,
                result_id="rslt-from-run" if status == "succeeded" else None,
                error_message=None,
            )

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            calls.append(f"arrow:{result_id}")
            return pa.table({"id": [1, 2, 3]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
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
    assert table.num_rows == 3
    # The id Arrow was fetched with came off the run, not the query reply.
    assert calls == [
        "query",
        "get_query_run:running",
        "get_query_run:succeeded",
        "arrow:rslt-from-run",
    ]


def test_unknown_run_status_keeps_polling_and_the_timeout_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrecognised run status waits rather than being called terminal.

    Treating an unknown status as terminal is the cheaper failure to diagnose
    and much the more expensive one to suffer: a single status added upstream
    would fail every read at once, where waiting costs one slow call. So the
    poll enumerates what it knows is terminal, and the timeout carries the
    status it last saw -- which is precisely what was missing while
    `interrupted` went unrecognised, and what would have made that a one-line
    diagnosis instead of a mystery.
    """

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> AsyncQueryResponse:
            return _async_query_response()

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            return SimpleNamespace(status="evicted", result_id=None, error_message=None)

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
    monkeypatch.setattr(mc.time, "sleep", lambda _seconds: None)

    client = mc.ManagedDatabaseClient(
        api_key="k",
        workspace_id="w",
        api_base_url="https://example.test",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = _fake_runtime()
    monkeypatch.setattr(client, "_QUERY_TIMEOUT_SECONDS", 0.05)

    # TimeoutError classifies as transient, so the retry wrapper re-raises it
    # as such once the budget is spent -- the status still has to reach the text.
    with pytest.raises(HotdataTransientError, match="evicted"):
        client.fetch_table(database="mydb", schema="public", table="orders")


def test_arrow_fetch_waits_out_a_result_that_is_not_ready_yet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Belt and braces over the run wait.

    A run reports `succeeded` only once its result is saved and ready, so this
    should not happen. Tolerating it costs nothing and removes the need to take
    that ordering on trust: the Arrow endpoint answers a result that is not ready
    with a small refusal rather than with data, so waiting here is cheap in the
    way waiting on the JSON result body is not.
    """
    attempts: list[str] = []

    class FakeQueryApi:
        def __init__(self, api: object) -> None:
            pass

        def query(self, request: object, *, x_database_id: str) -> AsyncQueryResponse:
            return _async_query_response()

    class FakeQueryRunsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_query_run(self, query_run_id: str, **kwargs: Any) -> Any:
            return SimpleNamespace(status="succeeded", result_id="rslt1", error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api: object) -> None:
            pass

        def get_result_arrow(self, result_id: str, **kwargs: Any) -> pa.Table:
            attempts.append(result_id)
            if len(attempts) < 3:
                raise ResultNotReadyError(status="processing", result_id=result_id)
            return pa.table({"id": [1, 2]})

    monkeypatch.setattr(mc, "QueryApi", FakeQueryApi)
    monkeypatch.setattr(mc, "QueryRunsApi", FakeQueryRunsApi)
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
    assert len(attempts) == 3
