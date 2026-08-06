"""Managed database helpers (Hotdata-owned catalogs with parquet table loads)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from hotdata.exceptions import ApiException

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
    names the *text* column a query passes to ``vector_distance(col, 'text')``;
    ``columns`` then holds the generated embedding column instead. It is ``None``
    for BM25, sorted, and plain (existing-vector-column) indexes.

    ``index_type``, ``columns``, and ``metric`` echo the requested values when
    the server does not return the built index alongside the finished job.
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
