"""Managed database helpers (Hotdata-owned catalogs with parquet table loads)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from hotdata.exceptions import ApiException
from hotdata.models.table_partition_key import TablePartitionKey
from hotdata.models.table_sort_key import TableSortKey

DEFAULT_SCHEMA = "public"


@dataclass(frozen=True)
class ManagedDatabase:
    id: str
    description: str | None
    default_connection_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ManagedTable:
    full_name: str
    schema: str
    table: str
    synced: bool
    last_sync: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TableLayout:
    """A managed table's declared storage layout, as the server reports it.

    Both lists carry the generated `TablePartitionKey` / `TableSortKey` models,
    in the order they were declared. A layout is fixed when the table is created
    and cannot be altered, so reading it back is the only way to confirm what was
    actually applied — which is why this exists as a first-class return rather
    than a field on `ManagedTable`, whose other fields describe sync state.

    Empty lists mean no layout was declared. That reading is only safe because
    this is resolved through a MANAGED database: the same fields on a table
    discovered from an external connection are empty because its layout belongs
    to the upstream system, which is "not known from here" rather than
    "confirmed none".
    """

    schema_name: str
    table_name: str
    partition_by: list[TablePartitionKey]
    sorted_by: list[TableSortKey]

    @property
    def is_partitioned(self) -> bool:
        return bool(self.partition_by)

    @property
    def is_sorted(self) -> bool:
        return bool(self.sorted_by)


@dataclass(frozen=True)
class LoadManagedTableResult:
    connection_id: str
    schema_name: str
    table_name: str
    row_count: int
    full_name: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CreateIndexResult:
    """An index created on a managed table.

    ``status`` is the server's own — ``"ready"`` once the index is built,
    ``"pending"`` while it is still building. ``job_id`` identifies the
    background build job, and is ``None`` only when the server built the index
    inline. A caller that passed ``wait=False`` always gets ``"pending"`` and
    owns checking the job's outcome.

    ``source_column`` is set only for an embedding-backed vector index, where it
    names the *text* column a query passes to ``vector_distance(col, 'text')``.
    It is ``None`` for BM25, sorted, and plain (existing-vector-column) indexes.

    ``index_type``, ``columns``, and ``metric`` echo the requested values when the
    server does not return the built index alongside the finished job — that is,
    on the ``wait=False`` path and when a finished job carries no index payload.
    Only when the server did return it does ``columns`` hold the *generated*
    embedding column for an embedding-backed index; on the echoing paths
    ``columns[0]`` is the source text column, the same value as
    ``source_column``. Read ``status`` to tell the cases apart.
    """

    full_name: str
    schema_name: str
    table_name: str
    index_name: str
    index_type: str
    columns: list[str]
    metric: str | None
    source_column: str | None
    status: str
    job_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def enum_value(value: Any) -> str:
    """Render an enum-or-str API field as its wire string.

    The generated models type several status fields as ``str``-mixin enums,
    whose ``str()`` is ``"JobStatus.FAILED"`` rather than ``"failed"``.
    """
    inner = getattr(value, "value", value)
    return str(inner)


def is_parquet_path(path: str) -> bool:
    return Path(path).suffix.lower() == ".parquet"


def managed_database_from_detail(detail: Any) -> ManagedDatabase:
    return ManagedDatabase(
        id=str(detail.id),
        description=detail.name,
        default_connection_id=str(detail.default_connection_id),
    )


def api_error_message(exc: ApiException) -> str:
    reason = exc.reason or str(exc)
    # Keep the response body: it carries the API's actual explanation.
    body = getattr(exc, "body", None)
    if body:
        return f"{reason}: {' '.join(str(body).split())[:500]}"
    return reason
