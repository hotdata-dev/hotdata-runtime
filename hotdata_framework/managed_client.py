"""Retry-wrapped managed-database client shared by Hotdata adapter packages.

Both hotdata-airflow and hotdata-dlt-destination import this module so that
the higher-level client logic (retries, Arrow queries, table management) lives
in one place rather than being duplicated per adapter.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

import pyarrow as pa
from hotdata.api.query_api import QueryApi
from hotdata.api.query_runs_api import QueryRunsApi
from hotdata.arrow import ResultNotReadyError
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
        arrow = ArrowResultsApi(self._runtime.api)
        deadline = time.monotonic() + self._QUERY_TIMEOUT_SECONDS
        while True:
            try:
                return arrow.get_result_arrow(result_id, x_database_id=database_id)
            except ResultNotReadyError:
                # Waiting on the run should already have made this unreachable:
                # a run reports `succeeded` only once its result is saved and
                # ready. Tolerating it anyway costs nothing and removes the need
                # to take that ordering on trust. The Arrow endpoint answers a
                # result that is not ready with a small refusal rather than with
                # data, so waiting here is cheap in the way waiting on the JSON
                # result body -- which is what this change removed -- is not.
                if time.monotonic() >= deadline:
                    raise
                time.sleep(self._POLL_INTERVAL_SECONDS)

    def _query_database_scoped(self, sql: str, *, database_id: str) -> str | None:
        raw = QueryApi(self._runtime.api).query(
            # Asked asynchronously because this caller wants a result id, not
            # rows. A synchronous submit always builds an inline preview of the
            # result and sends it -- megabytes, on a path that then reads the
            # whole result as Arrow anyway and never looks at the preview. The
            # async reply carries a run id and nothing else, and there is no way
            # to suppress the preview on a synchronous one.
            #
            # It also settles the types: the preview is JSON, which has no Arrow
            # schema and renders non-finite floats as null, so it could not have
            # substituted for the Arrow fetch even when it holds every row.
            #
            # `var_async` is the generated SDK's spelling of the wire field
            # `async`, which is a Python keyword and so cannot be the attribute
            # name.
            QueryRequest(sql=sql, var_async=True),
            x_database_id=database_id,
        )
        # Both reply shapes carry `query_run_id`, and the run is the readiness
        # signal for either -- a synchronous reply (which `async_after_ms` can
        # still produce) returns rows inline but goes on saving the full result
        # in the background, so it is not the finish line either.
        if isinstance(raw, (QueryResponse, AsyncQueryResponse)):
            return self._await_query_run(raw.query_run_id, database_id=database_id)
        return None

    def _await_query_run(self, query_run_id: str, *, database_id: str) -> str | None:
        """Wait for a query run to finish; return the result id it produced.

        The run is the whole wait. A run turns `succeeded` only after its result
        has been saved and is `ready`, so `succeeded` needs no second check
        against the result -- and asking the result endpoint instead would mean
        downloading the entire result to read one field, which the server
        refuses outright (413/429) once the result is large enough.

        `result_id` comes off the run rather than off the query reply because a
        `succeeded` run reports none when every row came back inline but the
        result could not be saved for later retrieval.
        """
        runs = QueryRunsApi(self._runtime.api)
        deadline = time.monotonic() + self._QUERY_TIMEOUT_SECONDS
        last_status: str | None = None
        while time.monotonic() < deadline:
            # Runs (like results) of database-scoped queries are database-scoped.
            run = runs.get_query_run(query_run_id, x_database_id=database_id)
            last_status = run.status
            if run.status == "succeeded":
                return run.result_id
            if run.status == "interrupted":
                # Terminal, but the server lost the run rather than rejecting
                # the query, so it is the one failure here worth re-running.
                # Raised pre-classified: `classify_sdk_error` cannot tell this
                # apart from an ordinary RuntimeError and would call it terminal.
                raise HotdataTransientError(
                    run.error_message or f"Query run {query_run_id} was interrupted"
                )
            if run.status == "failed":
                raise RuntimeError(run.error_message or f"Query run {query_run_id} failed")
            # Any other status keeps polling, including one this client has never
            # seen. Treating an unrecognised status as terminal is the cheaper
            # failure to diagnose and by far the more expensive one to suffer: a
            # single status added upstream would then fail every query at once,
            # where waiting costs one slow call. What made `interrupted`
            # expensive was not the waiting, it was that the timeout never said
            # which status it had waited on -- so the message now carries it.
            time.sleep(self._POLL_INTERVAL_SECONDS)
        raise TimeoutError(
            f"Query run {query_run_id} did not finish within "
            f"{self._QUERY_TIMEOUT_SECONDS}s (last status: {last_status})"
        )

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
