from __future__ import annotations

import functools
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any, Literal, get_args

from hotdata import ApiClient, Configuration
from hotdata.api.connections_api import ConnectionsApi
from hotdata.api.databases_api import DatabasesApi
from hotdata.api.indexes_api import IndexesApi
from hotdata.api.information_schema_api import InformationSchemaApi
from hotdata.api.jobs_api import JobsApi
from hotdata.api.query_api import QueryApi
from hotdata.api.query_runs_api import QueryRunsApi
from hotdata.api.results_api import ResultsApi
# The enriched wrapper (hotdata.uploads), NOT the generated hotdata.api class:
# it adds the full upload_file orchestration used by upload_parquet.
from hotdata.uploads import UploadError, UploadsApi
from hotdata.exceptions import ApiException
from hotdata.models.add_managed_table_request import AddManagedTableRequest
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.create_database_request import CreateDatabaseRequest
from hotdata.models.create_index_request import CreateIndexRequest
from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl
from hotdata.models.index_info_response import IndexInfoResponse
from hotdata.models.job_status_response import JobStatusResponse
from hotdata.models.load_managed_table_request import LoadManagedTableRequest
from hotdata.models.query_request import QueryRequest
from hotdata.models.query_response import QueryResponse
from hotdata.models.submit_job_response import SubmitJobResponse
from hotdata.models.table_info import TableInfo
from urllib3.exceptions import HTTPError as Urllib3HTTPError
from urllib3.exceptions import ProtocolError

from hotdata_framework.databases import (
    DEFAULT_SCHEMA,
    CreateIndexResult,
    LoadManagedTableResult,
    ManagedDatabase,
    ManagedTable,
    api_error_message,
    enum_value,
    is_parquet_path,
    managed_database_from_detail,
)
from hotdata_framework.env import (
    default_api_key,
    default_host,
    normalize_host,
    pick_workspace,
)
from hotdata_framework.http import default_http_retries
from hotdata_framework.result import QueryResult

# Load modes the managed-table endpoint accepts: replace overwrites, append adds
# rows, delete/update/upsert match by the table's declared key.
ManagedLoadMode = Literal["replace", "append", "delete", "update", "upsert"]

# Index kinds the indexes endpoint accepts. "sorted" is the server-side default;
# "bm25" backs full-text search and "vector" backs nearest-neighbour search.
IndexType = Literal["sorted", "bm25", "vector"]

# Distance metrics a vector index can be built with. Each one accelerates
# exactly one query function: cosine -> cosine_distance, l2 -> l2_distance,
# dot -> negative_dot_product. A hand-written query naming a different function
# falls back to a full scan; the provider-backed vector_distance path resolves
# the function from the index instead, so it cannot mismatch.
VectorMetric = Literal["l2", "cosine", "dot"]

_INDEX_TYPES = frozenset(get_args(IndexType))
_VECTOR_METRICS = frozenset(get_args(VectorMetric))

_TERMINAL = frozenset({"succeeded", "failed", "cancelled"})
_RESULT_FAILURE = frozenset({"failed", "cancelled"})
# Jobs have no "cancelled" state; "partially_succeeded" carries an error_message.
_JOB_TERMINAL = frozenset({"succeeded", "partially_succeeded", "failed"})


@dataclass(frozen=True)
class ResultSummary:
    result_id: str
    status: str
    created_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunHistoryItem:
    query_run_id: str
    status: str
    created_at: str | None
    execution_time_ms: int | None
    result_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)



def apply_default_request_timeout(
    api_client: ApiClient, timeout: float | tuple[float, float]
) -> None:
    """Give every request through this client a socket-level deadline.

    The generated client forwards ``_request_timeout=None`` — urllib3's
    no-timeout — on every call unless the caller passes one explicitly, and
    most helper methods expose no such knob. A stalled or black-holed server
    therefore blocks the calling thread indefinitely. Wrapping the REST seam
    applies ``timeout`` (seconds, or a ``(connect, read)`` pair) as the
    default while still honoring an explicit per-call ``_request_timeout``.
    """
    rest_client = api_client.rest_client
    original = rest_client.request

    @functools.wraps(original)
    def request_with_default_timeout(
        method,
        url,
        headers=None,
        body=None,
        post_params=None,
        _request_timeout=None,
    ):
        if _request_timeout is None:
            _request_timeout = timeout
        return original(
            method,
            url,
            headers=headers,
            body=body,
            post_params=post_params,
            _request_timeout=_request_timeout,
        )

    rest_client.request = request_with_default_timeout


class HotdataClient:
    """Thin wrapper around the Hotdata Python SDK with query polling helpers."""

    def __init__(
        self,
        api_key: str,
        workspace_id: str,
        *,
        host: str | None = None,
        request_timeout: float | tuple[float, float] | None = None,
    ) -> None:
        self._host = normalize_host(host) if host else default_host()
        self._api_key = api_key
        self._workspace_id = workspace_id
        self._config = Configuration(
            host=self._host,
            api_key=api_key,
            workspace_id=workspace_id,
            retries=default_http_retries(),
        )
        self._api = ApiClient(self._config)
        if request_timeout is not None:
            apply_default_request_timeout(self._api, request_timeout)

    @classmethod
    def from_env(cls) -> HotdataClient:
        api_key = default_api_key()
        if not api_key:
            raise RuntimeError("HOTDATA_API_KEY must be set.")
        host = default_host()
        workspace_id = pick_workspace(api_key, host)
        return cls(api_key, workspace_id, host=host)

    @property
    def workspace_id(self) -> str:
        return self._workspace_id

    @property
    def host(self) -> str:
        return self._host

    @property
    def api(self) -> ApiClient:
        return self._api

    def close(self) -> None:
        self._api.close()

    def __enter__(self) -> HotdataClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def connections(self) -> ConnectionsApi:
        return ConnectionsApi(self._api)

    def _databases_api(self) -> DatabasesApi:
        return DatabasesApi(self._api)

    def _information_schema(self) -> InformationSchemaApi:
        return InformationSchemaApi(self._api)

    def _indexes_api(self) -> IndexesApi:
        return IndexesApi(self._api)

    def _jobs_api(self) -> JobsApi:
        return JobsApi(self._api)

    def _query_api(self) -> QueryApi:
        return QueryApi(self._api)

    def _query_runs_api(self) -> QueryRunsApi:
        return QueryRunsApi(self._api)

    def _results_api(self) -> ResultsApi:
        return ResultsApi(self._api)

    def query_runs(self) -> QueryRunsApi:
        return self._query_runs_api()

    def results(self) -> ResultsApi:
        return self._results_api()

    def uploads(self) -> UploadsApi:
        return UploadsApi(self._api)

    def list_managed_databases(self) -> list[ManagedDatabase]:
        listing = self._databases_api().list_databases()
        result: list[ManagedDatabase] = []
        for summary in listing.databases:
            try:
                detail = self._databases_api().get_database(summary.id)
                result.append(managed_database_from_detail(detail))
            except ApiException:
                pass
        return result

    def resolve_managed_database(self, name_or_id: str) -> ManagedDatabase:
        # Try direct ID lookup first
        try:
            detail = self._databases_api().get_database(name_or_id)
            return managed_database_from_detail(detail)
        except ApiException as e:
            if e.status != 404:
                raise RuntimeError(api_error_message(e)) from e

        # Fall back to description-based lookup
        listing = self._databases_api().list_databases()
        match_id: str | None = None
        for db in listing.databases:
            if db.name == name_or_id:
                match_id = db.id
                break
        if match_id is None:
            raise KeyError(f"No database named or with id {name_or_id!r}")
        try:
            detail = self._databases_api().get_database(match_id)
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e
        return managed_database_from_detail(detail)

    def _as_managed_database(self, database: str | ManagedDatabase) -> ManagedDatabase:
        """Return ``database`` as-is if it is already a resolved ``ManagedDatabase``,
        otherwise resolve it by name or id.

        Passing an already-resolved ``ManagedDatabase`` (e.g. the value returned by
        :meth:`create_managed_database`) skips the id/name read probe, so callers
        whose API key may create but not read ``/databases`` can drive loads without
        a forbidden read.
        """
        if isinstance(database, ManagedDatabase):
            return database
        return self.resolve_managed_database(database)

    def create_managed_database(
        self,
        description: str | None = None,
        *,
        schema: str = DEFAULT_SCHEMA,
        tables: list[str] | None = None,
        keys: dict[str, list[str]] | None = None,
        expires_at: str | None = None,
    ) -> ManagedDatabase:
        """Create a managed database. ``keys`` maps a table to its key columns
        (enabling delete/update/upsert on it); omitted tables are keyless."""
        keys = keys or {}
        schemas = None
        if tables:
            schemas = [
                DatabaseDefaultSchemaDecl(
                    name=schema,
                    tables=[
                        DatabaseDefaultTableDecl(name=t, key=list(keys.get(t, [])))
                        for t in tables
                    ],
                )
            ]
        request = CreateDatabaseRequest(
            name=description,
            schemas=schemas,
            expires_at=expires_at,
        )
        try:
            created = self._databases_api().create_database(request)
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e
        return managed_database_from_detail(created)

    def delete_managed_database(self, name_or_id: str | ManagedDatabase) -> None:
        db = self._as_managed_database(name_or_id)
        try:
            self._databases_api().delete_database(db.id)
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e

    def list_managed_tables(
        self,
        database: str | ManagedDatabase,
        *,
        schema: str | None = None,
    ) -> list[ManagedTable]:
        db = self._as_managed_database(database)
        rows: list[ManagedTable] = []
        for t in self.iter_tables(connection_id=db.default_connection_id):
            if schema is not None and t.var_schema != schema:
                continue
            rows.append(
                ManagedTable(
                    full_name=f"{db.id}.{t.var_schema}.{t.table}",
                    schema=t.var_schema,
                    table=t.table,
                    synced=t.synced,
                    last_sync=t.last_sync,
                )
            )
        rows.sort(key=lambda row: (row.schema, row.table))
        return rows

    def upload_parquet(self, path: str) -> str:
        """Upload a parquet file via the SDK's upload orchestration.

        ``UploadsApi.upload_file`` owns the whole session -> storage PUT ->
        finalize flow: concurrent part uploads under a peak-memory budget,
        per-part retries, and ETag/size validation. Errors surface with the
        underlying ``ApiException`` as the direct cause when there is one, so
        retry classification keeps seeing the status code.
        """
        if not is_parquet_path(path):
            raise ValueError(f"Managed table loads require a parquet file (got {path!r})")
        try:
            finalized = self.uploads().upload_file(
                path, content_type="application/octet-stream"
            )
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e
        except UploadError as e:
            node: BaseException | None = e
            while node is not None and not isinstance(node, ApiException):
                node = node.__cause__
            if isinstance(node, ApiException):
                raise RuntimeError(api_error_message(node)) from node
            raise RuntimeError(str(e)) from e
        return finalized.upload_id

    def load_managed_table(
        self,
        database: str | ManagedDatabase,
        table: str,
        *,
        schema: str = DEFAULT_SCHEMA,
        upload_id: str | None = None,
        file: str | None = None,
        mode: ManagedLoadMode = "replace",
        key: list[str] | None = None,
    ) -> LoadManagedTableResult:
        if (upload_id is None) == (file is None):
            raise ValueError("Exactly one of upload_id or file is required")
        db = self._as_managed_database(database)
        if upload_id is not None:
            resolved_upload_id = upload_id
        else:
            assert file is not None
            resolved_upload_id = self.upload_parquet(file)
        request = LoadManagedTableRequest(
            mode=mode,
            upload_id=resolved_upload_id,
            key=key,
        )
        try:
            loaded = self.connections().load_managed_table(
                db.default_connection_id,
                schema,
                table,
                request,
            )
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e
        return LoadManagedTableResult(
            connection_id=loaded.connection_id,
            schema_name=loaded.schema_name,
            table_name=loaded.table_name,
            row_count=loaded.row_count,
            full_name=f"{db.id}.{loaded.schema_name}.{loaded.table_name}",
        )

    def add_managed_table(
        self,
        database: str | ManagedDatabase,
        table: str,
        *,
        schema: str = DEFAULT_SCHEMA,
        key: list[str] | None = None,
    ) -> ManagedTable:
        """Declare a new table on an existing managed database.

        The table is added empty (declared-but-unloaded); populate it with
        :meth:`load_managed_table`. Use this to evolve a managed database's
        schema after creation without recreating it. ``key`` sets the
        row-identity columns for delete/update/upsert; omit for keyless.
        """
        db = self._as_managed_database(database)
        request = AddManagedTableRequest(name=table, key=list(key or []))
        try:
            self._databases_api().add_database_table(db.id, schema, request)
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e
        return ManagedTable(
            full_name=f"{db.id}.{schema}.{table}",
            schema=schema,
            table=table,
            synced=False,
            last_sync=None,
        )

    def delete_managed_table(
        self,
        database: str | ManagedDatabase,
        table: str,
        *,
        schema: str = DEFAULT_SCHEMA,
    ) -> None:
        db = self._as_managed_database(database)
        try:
            self.connections().delete_managed_table(db.default_connection_id, schema, table)
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e

    def create_index(
        self,
        database: str | ManagedDatabase,
        table: str,
        *,
        schema: str = DEFAULT_SCHEMA,
        index_name: str | None = None,
        columns: list[str],
        index_type: IndexType,
        metric: VectorMetric | None = None,
        dimensions: int | None = None,
        embedding_provider_id: str | None = None,
        output_column: str | None = None,
        description: str | None = None,
        wait: bool = True,
        timeout_s: float = 300.0,
        poll_interval_s: float = 2.0,
    ) -> CreateIndexResult:
        """Build an index on a managed table and wait for it to be ready.

        The Python equivalent of ``hotdata indexes create``, scoped to managed
        databases. Indexing a table on a plain (non-managed) connection is not
        supported here; the CLI's ``--catalog`` flag covers that case.

        ``index_type`` selects the index kind and is required: ``"bm25"`` for
        full-text search (queries error outright without one), ``"vector"`` for
        nearest-neighbour search (queries work without one, but only at
        full-scan speed), or ``"sorted"``. The API defaults an unspecified kind
        to ``"sorted"``; this method makes the choice explicit instead, because
        the wrong kind fails at query time rather than here.

        ``index_name`` defaults to ``{table}_{columns}_{index_type}``, the same
        derivation the CLI uses when ``--name`` is omitted, so both surfaces
        name the same index identically.

        There are two kinds of vector index, and they are queried differently:

        * **Plain** — omit ``embedding_provider_id``. ``columns`` is the existing
          vector column (a float list), and a query passes a literal vector:
          ``cosine_distance(col, ARRAY[...])``. Here ``metric`` must match the
          distance function the caller writes — ``cosine`` serves
          ``cosine_distance``, ``l2`` serves ``l2_distance``, ``dot`` serves
          ``negative_dot_product``. A mismatch is not an error: the query
          silently reverts to a full table scan. Omitting ``metric`` lets the
          server choose (``l2`` for float-array columns), so pass it explicitly
          whenever the query function is known.
        * **Provider-backed** — set ``embedding_provider_id`` (e.g. the system
          provider ``sys_emb_openai``). ``columns`` is then the *source text*
          column; the provider embeds it into ``output_column`` (default
          ``{column}_embedding``) and the index is built over that. A query
          passes text, not a vector — ``vector_distance(source_col, 'query')`` —
          and the server resolves the matching distance function from the index
          itself, so the metric-mismatch trap above does not apply. The returned
          ``source_column`` names the column to query.

        ``dimensions`` picks the output width for providers that support several;
        it does not apply when indexing an existing vector column, whose width is
        read from the data. ``description`` is a user-facing label for the
        embedding (e.g. ``"product descriptions"``), stored alongside it. A vector
        index takes exactly one column, and every option in this paragraph — plus
        ``metric`` — is rejected for a non-vector ``index_type``, matching the CLI.

        The server builds the index as a background job. This method polls that
        job to a terminal state and raises ``RuntimeError`` if it failed, because
        the submit call itself reports success for builds that later fail. Pass
        ``wait=False`` to return as soon as the job is accepted — the result then
        carries ``status="pending"`` and a ``job_id``, and the caller owns
        checking the outcome (the CLI's ``--async`` plus ``hotdata jobs``).

        Raises ``ValueError`` for an unusable argument combination,
        ``RuntimeError`` if the API rejects the request or the build fails, and
        ``TimeoutError`` if the build is still running after ``timeout_s``.
        """
        if not columns:
            raise ValueError("create_index requires at least one column")
        if index_type not in _INDEX_TYPES:
            allowed = ", ".join(sorted(_INDEX_TYPES))
            raise ValueError(f"index_type must be one of {allowed} (got {index_type!r})")
        if index_type != "vector":
            vector_only = {
                "metric": metric,
                "dimensions": dimensions,
                "embedding_provider_id": embedding_provider_id,
                "output_column": output_column,
                "description": description,
            }
            supplied = sorted(k for k, v in vector_only.items() if v is not None)
            if supplied:
                raise ValueError(
                    f"{', '.join(supplied)} appl{'ies' if len(supplied) == 1 else 'y'} to "
                    f"vector indexes only (index_type={index_type!r})"
                )
        else:
            if len(columns) != 1:
                raise ValueError(
                    f"a vector index takes exactly one column (got {len(columns)}); "
                    "the engine indexes only the first"
                )
            if metric is not None and metric not in _VECTOR_METRICS:
                allowed = ", ".join(sorted(_VECTOR_METRICS))
                raise ValueError(f"metric must be one of {allowed} (got {metric!r})")

        # Matches the CLI's derivation so both surfaces name the same index
        # identically: `hotdata indexes create` without --name.
        resolved_name = index_name or f"{table}_{'_'.join(columns)}_{index_type}"

        db = self._as_managed_database(database)
        request = CreateIndexRequest(
            index_name=resolved_name,
            columns=list(columns),
            index_type=index_type,
            metric=metric,
            dimensions=dimensions,
            embedding_provider_id=embedding_provider_id,
            output_column=output_column,
            description=description,
            var_async=True,
        )
        try:
            submitted = self._indexes_api().create_index(
                db.default_connection_id,
                schema,
                table,
                request,
            )
        except ApiException as e:
            raise RuntimeError(api_error_message(e)) from e

        full_name = f"{db.id}.{schema}.{table}"

        # A build the server finished inline answers 201 with the index itself;
        # the async path answers 202 with a job to poll.
        if isinstance(submitted, IndexInfoResponse):
            return self._index_result(submitted, full_name, schema, table, job_id=None)

        if not isinstance(submitted, SubmitJobResponse):
            raise RuntimeError(f"Unexpected create_index response type: {type(submitted)!r}")

        job_id = submitted.id

        def requested_result(status: str) -> CreateIndexResult:
            """Echo the requested values, for the paths where the server hands
            back a job rather than the built index."""
            return CreateIndexResult(
                full_name=full_name,
                schema_name=schema,
                table_name=table,
                index_name=resolved_name,
                index_type=index_type,
                columns=list(columns),
                metric=metric,
                source_column=columns[0] if embedding_provider_id else None,
                status=status,
                job_id=job_id,
            )

        if not wait:
            return requested_result(enum_value(submitted.status))

        job = self._poll_job(job_id, timeout_s=timeout_s, interval_s=poll_interval_s)
        status = enum_value(job.status)
        if status != "succeeded":
            detail = job.error_message or f"Index build {status}"
            raise RuntimeError(f"Index {resolved_name!r} on {full_name}: {detail}")

        # `result` is a oneOf wrapper today; tolerate the model arriving directly.
        built = getattr(job.result, "actual_instance", job.result)
        if isinstance(built, IndexInfoResponse):
            return self._index_result(built, full_name, schema, table, job_id=job_id)
        return requested_result("ready")

    @staticmethod
    def _index_result(
        info: IndexInfoResponse,
        full_name: str,
        schema: str,
        table: str,
        *,
        job_id: str | None,
    ) -> CreateIndexResult:
        return CreateIndexResult(
            full_name=full_name,
            schema_name=schema,
            table_name=table,
            index_name=info.index_name,
            index_type=info.index_type,
            columns=list(info.columns),
            metric=info.metric,
            source_column=info.source_column,
            status=enum_value(info.status),
            job_id=job_id,
        )

    def list_recent_results(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ResultSummary]:
        listing = self.results().list_results(limit=limit, offset=offset)
        return [
            ResultSummary(
                result_id=r.id,
                status=r.status,
                created_at=r.created_at,
            )
            for r in listing.results
        ]

    def list_run_history(
        self,
        *,
        limit: int = 20,
    ) -> list[RunHistoryItem]:
        listing = self.query_runs().list_query_runs(limit=limit)
        return [
            RunHistoryItem(
                query_run_id=r.id,
                status=r.status,
                created_at=r.created_at,
                execution_time_ms=r.execution_time_ms,
                result_id=r.result_id,
            )
            for r in listing.query_runs
        ]

    def iter_tables(
        self,
        *,
        connection_id: str | None = None,
        include_columns: bool = False,
        page_size: int = 200,
    ) -> Iterator[TableInfo]:
        cursor: str | None = None
        while True:
            resp = self._information_schema().information_schema(
                connection_id=connection_id,
                include_columns=include_columns,
                limit=page_size,
                cursor=cursor,
            )
            yield from resp.tables
            if not resp.has_more or not resp.next_cursor:
                break
            cursor = resp.next_cursor

    def qualified_table_name(self, t: TableInfo) -> str:
        return f"{t.connection}.{t.var_schema}.{t.table}"

    def list_qualified_table_names(
        self, *, limit: int = 5000, connection_id: str | None = None
    ) -> list[str]:
        out: list[str] = []
        for t in self.iter_tables(connection_id=connection_id):
            out.append(self.qualified_table_name(t))
            if len(out) >= limit:
                break
        return sorted(out)

    def connection_id_by_name(self) -> dict[str, str]:
        listing = self.connections().list_connections()
        id_map: dict[str, str] = {}
        duplicate_names: set[str] = set()
        for c in listing.connections:
            if c.name in id_map and id_map[c.name] != c.id:
                duplicate_names.add(c.name)
            id_map[c.name] = c.id
        if duplicate_names:
            names = ", ".join(sorted(duplicate_names))
            raise RuntimeError(
                f"Duplicate connection names found: {names}. Use an explicit connection_id."
            )
        return id_map

    def columns_for_qualified(
        self,
        qualified: str,
        *,
        connection_id: str | None = None,
    ) -> list[TableInfo]:
        parts = qualified.split(".")
        if len(parts) < 3:
            raise ValueError(f"Expected connection.schema.table, got {qualified!r}")
        conn_name, schema_name, table_name = (
            parts[0],
            parts[1],
            ".".join(parts[2:]),
        )
        conn_id = connection_id
        if conn_id is None:
            id_map = self.connection_id_by_name()
            conn_id = id_map.get(conn_name)
            if not conn_id:
                raise KeyError(f"Unknown connection {conn_name!r}")
        resp = self._information_schema().information_schema(
            connection_id=conn_id,
            var_schema=schema_name,
            table=table_name,
            include_columns=True,
            limit=10,
        )
        if not resp.tables:
            return []
        first = resp.tables[0]
        return first.columns or []

    def _poll_query_run(
        self,
        query_run_id: str,
        *,
        timeout_s: float = 300.0,
        interval_s: float = 0.5,
    ):
        runs = self._query_runs_api()
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            last = runs.get_query_run(query_run_id)
            if last.status in _TERMINAL:
                return last
            time.sleep(interval_s)
        raise TimeoutError(
            f"Query run {query_run_id} did not finish within {timeout_s}s "
            f"(last status: {getattr(last, 'status', None)})"
        )

    def _poll_job(
        self,
        job_id: str,
        *,
        timeout_s: float = 300.0,
        interval_s: float = 2.0,
    ) -> JobStatusResponse:
        jobs = self._jobs_api()
        deadline = time.monotonic() + timeout_s
        last: JobStatusResponse | None = None
        while time.monotonic() < deadline:
            try:
                last = jobs.get_job(job_id)
            except ApiException as e:
                raise RuntimeError(api_error_message(e)) from e
            if last.status in _JOB_TERMINAL:
                return last
            time.sleep(interval_s)
        last_status = enum_value(last.status) if last is not None else None
        raise TimeoutError(
            f"Job {job_id} did not finish within {timeout_s}s (last status: {last_status})"
        )

    def _wait_result_ready(
        self,
        result_id: str,
        *,
        timeout_s: float = 300.0,
        interval_s: float = 0.5,
    ):
        results = self._results_api()
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            last = results.get_result(result_id)
            if last.status == "ready":
                return last
            if last.status in _RESULT_FAILURE:
                raise RuntimeError(last.error_message or f"Result {last.status}")
            time.sleep(interval_s)
        raise TimeoutError(
            f"Result {result_id} not ready within {timeout_s}s "
            f"(last status: {getattr(last, 'status', None)})"
        )

    def execute_sql(
        self, sql: str, *, database: str | ManagedDatabase | None = None
    ) -> QueryResult:
        """Execute SQL and return a :class:`QueryResult`.

        Pass ``database`` to scope the query to a managed database.  A name or
        id is resolved to a database ID once before the retry loop; an
        already-resolved ``ManagedDatabase`` is used as-is (no read probe).  The
        ``X-Database-Id`` header is sent with every attempt.  Inside a managed
        database the built-in catalog is always ``"default"``, so table
        references should use ``"default"."<schema>"."<table>"``.
        """
        database_id = self._as_managed_database(database).id if database else None
        last_err: BaseException | None = None
        for attempt in range(3):
            try:
                return self._execute_sql_once(sql, database_id=database_id)
            except (ProtocolError, ConnectionResetError, Urllib3HTTPError) as e:
                last_err = e
                if attempt == 2:
                    raise
                time.sleep(0.2 * (2**attempt))
        raise last_err  # pragma: no cover

    def _execute_sql_once(self, sql: str, *, database_id: str | None = None) -> QueryResult:
        q = self._query_api()
        try:
            if database_id:
                raw = q.query(QueryRequest(sql=sql), x_database_id=database_id)
            else:
                raw = q.query(QueryRequest(sql=sql))
        except ApiException as e:
            raise RuntimeError(e.reason or str(e)) from e

        if isinstance(raw, AsyncQueryResponse):
            run = self._poll_query_run(raw.query_run_id)
            if run.status != "succeeded":
                raise RuntimeError(run.error_message or f"Query failed ({run.status})")
            if run.result_id:
                persisted = self._wait_result_ready(run.result_id)
                return QueryResult.from_get_result(persisted)
            raise RuntimeError("Query succeeded but no result_id was returned.")

        if isinstance(raw, QueryResponse):
            return QueryResult.from_query_response(raw)

        raise RuntimeError(f"Unexpected query response type: {type(raw)!r}")

    def get_result(self, result_id: str) -> QueryResult:
        r = self._results_api().get_result(result_id)
        if r.status != "ready":
            r = self._wait_result_ready(result_id)
        return QueryResult.from_get_result(r)


def from_env() -> HotdataClient:
    return HotdataClient.from_env()
