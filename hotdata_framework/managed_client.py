"""Retry-wrapped managed-database client shared by Hotdata adapter packages.

Both hotdata-airflow and hotdata-dlt-destination import this module so that
the higher-level client logic (retries, Arrow queries, table management) lives
in one place rather than being duplicated per adapter.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

import pyarrow as pa
from hotdata.api.query_api import QueryApi
from hotdata.api.query_runs_api import QueryRunsApi
from hotdata.api.results_api import ResultsApi
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_request import QueryRequest
from hotdata.models.query_response import QueryResponse

from hotdata_framework.client import HotdataClient as RuntimeClient
from hotdata_framework.client import ManagedLoadMode
from hotdata_framework.databases import LoadManagedTableResult, ManagedDatabase
from hotdata_framework.errors import (
    HotdataTransientError,
    classify_sdk_error,
)

T = TypeVar("T")


class _StatusResponse(Protocol):
    """Async resources (query runs, results) expose a status and error message."""

    status: str
    error_message: str | None


S = TypeVar("S", bound=_StatusResponse)


class ManagedDatabaseClient:
    """Managed-database client with bounded retries over hotdata-framework.

    This is the shared client used by Hotdata adapter packages (Airflow,
    dlt, etc.).  It wraps the lower-level RuntimeClient with retry logic,
    Arrow-based result fetching, and convenience helpers for the managed
    database lifecycle.
    """

    _QUERY_TIMEOUT_SECONDS = 300.0
    _POLL_INTERVAL_SECONDS = 0.4
    _MAX_BACKOFF_SECONDS = 30.0
    # Spread as a fraction of the wait, added on top of it. Half an interval is
    # enough to decorrelate writers that started together without materially
    # changing how long the budget lasts.
    _RETRY_JITTER_FRACTION = 0.5

    def __init__(
        self,
        *,
        api_key: str,
        workspace_id: str,
        api_base_url: str,
        max_retries: int,
        retry_backoff_seconds: float,
        request_timeout: float | tuple[float, float] | None = None,
    ) -> None:
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._runtime = RuntimeClient(
            api_key,
            workspace_id,
            host=api_base_url.rstrip("/"),
            request_timeout=request_timeout,
        )

    def close(self) -> None:
        self._runtime.close()

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: list[str],
        create_if_missing: bool,
    ) -> ManagedDatabase:
        def operation() -> ManagedDatabase:
            try:
                return self._runtime.resolve_managed_database(name)
            except KeyError:
                if not create_if_missing:
                    raise
                return self._runtime.create_managed_database(
                    description=name,
                    schema=schema,
                    tables=sorted(set(tables)),
                )

        return self._request_with_retry(operation)

    def table_is_synced(self, database: str, table: str, *, schema: str) -> bool:
        for managed_table in self._runtime.list_managed_tables(database, schema=schema):
            if managed_table.table == table:
                return managed_table.synced
        return False

    def fetch_table(self, *, database: str, schema: str, table: str) -> pa.Table | None:
        def operation() -> pa.Table | None:
            if not self.table_is_synced(database, table, schema=schema):
                return None
            db = self._runtime.resolve_managed_database(database)
            sql = f'SELECT * FROM "default"."{schema}"."{table}"'
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return None
            return self._fetch_result_arrow(result_id, database_id=db.id)

        return self._request_with_retry(operation)

    def _fetch_result_arrow(self, result_id: str, *, database_id: str) -> pa.Table:
        """Fetch a ready result as Arrow, carrying the database scope.

        Results of database-scoped queries are themselves database-scoped —
        the results endpoints reject requests without the scope. The hotdata
        0.6.0 SDK exposes (and requires) ``x_database_id`` on the Arrow
        helper directly.
        """
        return ArrowResultsApi(self._runtime.api).get_result_arrow(
            result_id, x_database_id=database_id
        )

    def _poll(
        self,
        fetch: Callable[[], S],
        *,
        is_ready: Callable[[S], bool],
        describe: str,
    ) -> S:
        """Poll ``fetch`` until ``is_ready`` is satisfied, or raise on failure/timeout.

        ``failed``/``cancelled`` statuses raise ``RuntimeError``; exceeding
        :attr:`_QUERY_TIMEOUT_SECONDS` raises ``TimeoutError``.
        """
        deadline = time.monotonic() + self._QUERY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            obj = fetch()
            if obj.status in ("failed", "cancelled"):
                raise RuntimeError(obj.error_message or f"{describe} {obj.status}")
            if is_ready(obj):
                return obj
            time.sleep(self._POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"{describe} timed out after {self._QUERY_TIMEOUT_SECONDS}s")

    def _query_database_scoped(self, sql: str, *, database_id: str) -> str | None:
        raw = QueryApi(self._runtime.api).query(
            QueryRequest(sql=sql),
            x_database_id=database_id,
        )
        if isinstance(raw, QueryResponse):
            # A synchronous response still persists its full result out-of-band
            # under ``result_id``; that result may be ``processing`` when the
            # inline preview returns, so wait for ``ready`` before the caller
            # fetches it as Arrow.
            return self._wait_result_ready(raw.result_id, database_id=database_id)
        if isinstance(raw, AsyncQueryResponse):
            run_result = self._await_query_run(raw.query_run_id, database_id=database_id)
            return self._wait_result_ready(run_result, database_id=database_id)
        return None

    def _await_query_run(self, query_run_id: str, *, database_id: str) -> str | None:
        runs = QueryRunsApi(self._runtime.api)
        run = self._poll(
            # Runs (like results) of database-scoped queries are database-scoped.
            lambda: runs.get_query_run(query_run_id, x_database_id=database_id),
            is_ready=lambda r: r.status == "succeeded",
            describe="Query",
        )
        return run.result_id

    def _wait_result_ready(self, result_id: str | None, *, database_id: str) -> str | None:
        if result_id is None:
            return None
        results = ResultsApi(self._runtime.api)
        self._poll(
            # The stored result of a database-scoped query 400s without the
            # database scope.
            lambda: results.get_result(result_id, x_database_id=database_id),
            is_ready=lambda r: r.status == "ready",
            describe=f"Result {result_id}",
        )
        return result_id

    def fetch_table_rows(self, *, database: str, schema: str, table: str) -> list[dict[str, Any]]:
        result = self.fetch_table(database=database, schema=schema, table=table)
        return result.to_pylist() if result is not None else []

    def upload_parquet(self, path: str) -> str:
        return self._request_with_retry(lambda: self._runtime.upload_parquet(path))

    def load_managed_table(
        self,
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
        mode: ManagedLoadMode = "replace",
        key: list[str] | None = None,
    ) -> LoadManagedTableResult:
        # Retryable in every mode, append included. A retry re-sends the SAME
        # upload_id, and the server keys a receipt on it: a replay returns the
        # committed result rather than applying the load a second time. So the
        # invariant that makes this safe is the upload id, not the mode — a
        # caller that re-stages the upload between attempts mints a new id,
        # loses the receipt, and a retried append would then duplicate rows.
        # This client stages once, in upload_parquet, outside the operation
        # retried here. `HotdataClient.load_managed_table(file=...)` uploads
        # inside the call and so does not hold the invariant; it is unwrapped,
        # and retrying an append through it is the caller's to justify.
        #
        # `key` is the merge key for delete/update/upsert loads: when set it is
        # matched per-load instead of a key declared at table creation. Omit it
        # to use the table's declared key. Ignored for replace/append.
        return self._request_with_retry(
            lambda: self._runtime.load_managed_table(
                database,
                table,
                schema=schema,
                upload_id=upload_id,
                mode=mode,
                key=key,
            )
        )

    def _request_with_retry(self, operation: Callable[[], T]) -> T:
        max_attempts = self._max_retries
        for attempt in range(1, max_attempts + 1):
            try:
                return operation()
            except Exception as error:
                mapped_error = classify_sdk_error(error.__cause__ or error)
                if isinstance(mapped_error, HotdataTransientError) and attempt < max_attempts:
                    time.sleep(self._retry_delay(attempt, mapped_error.retry_after_seconds))
                    continue
                raise mapped_error from error
        raise RuntimeError("No retry attempts configured")

    def _retry_delay(self, attempt: int, retry_after_seconds: float | None) -> float:
        """A linear ramp, floored by the server's Retry-After and spread by jitter.

        Retry-After is a floor rather than a replacement: it says how long the
        condition just refused typically lasts, while the ramp is what gives up
        eventually, and taking the larger of the two honours both. It is capped
        like the ramp so a hostile or mistaken header cannot park an attempt for
        an hour.

        Jitter is added on top and never subtracted, so a stated Retry-After is
        not undercut. It matters because the callers that collide are the ones
        that started together: writers refused by one table's lock would retry
        in lockstep on an identical ramp and re-collide every time.
        _MAX_BACKOFF_SECONDS caps the ramp, deliberately not the jitter above
        it — clamping the total would flatten every late attempt onto the same
        value and re-correlate exactly the waits that most need spreading.
        """
        base = min(self._retry_backoff_seconds * attempt, self._MAX_BACKOFF_SECONDS)
        if retry_after_seconds is not None:
            base = max(base, min(retry_after_seconds, self._MAX_BACKOFF_SECONDS))
        return base * (1.0 + random.random() * self._RETRY_JITTER_FRACTION)
