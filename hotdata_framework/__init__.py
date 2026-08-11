"""Hotdata runtime primitives for notebook, app, and adapter integrations."""

from importlib.metadata import PackageNotFoundError, version

from hotdata.models.table_partition_key import TablePartitionKey
from hotdata.models.table_sort_key import TableSortKey

from hotdata_framework.client import (
    HotdataClient,
    ResultSummary,
    RunHistoryItem,
    from_env,
)
from hotdata_framework.databases import (
    DEFAULT_SCHEMA,
    CreateIndexResult,
    LoadManagedTableResult,
    ManagedDatabase,
    ManagedTable,
    TableLayout,
    is_parquet_path,
)
from hotdata_framework.env import (
    WorkspaceSelection,
    default_api_key,
    default_host,
    explicit_workspace_id,
    list_workspaces,
    normalize_host,
    pick_workspace,
    resolve_workspace_selection,
)
from hotdata_framework.errors import (
    HotdataError,
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)
from hotdata_framework.health import workspace_health_lines
from hotdata_framework.managed_client import ManagedDatabaseClient
from hotdata_framework.result import QueryResult

try:
    __version__ = version("hotdata-framework")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "DEFAULT_SCHEMA",
    "CreateIndexResult",
    "HotdataClient",
    "HotdataError",
    "HotdataTerminalError",
    "HotdataTransientError",
    "LoadManagedTableResult",
    "ManagedDatabase",
    "ManagedDatabaseClient",
    "ManagedTable",
    "QueryResult",
    "ResultSummary",
    "RunHistoryItem",
    "TableLayout",
    "TablePartitionKey",
    "TableSortKey",
    "WorkspaceSelection",
    "__version__",
    "classify_sdk_error",
    "default_api_key",
    "default_host",
    "explicit_workspace_id",
    "from_env",
    "is_parquet_path",
    "list_workspaces",
    "normalize_host",
    "pick_workspace",
    "resolve_workspace_selection",
    "workspace_health_lines",
]
