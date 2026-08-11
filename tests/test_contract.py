from __future__ import annotations

from dataclasses import fields
from unittest.mock import patch

import hotdata_framework as hr
from hotdata_framework.client import HotdataClient
from hotdata_framework.result import QueryResult


def test_public_exports_contract():
    assert hr.__all__ == [
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


def test_module_from_env_delegates_to_client_classmethod():
    sentinel = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    with patch.object(HotdataClient, "from_env", return_value=sentinel) as m:
        got = hr.from_env()
    m.assert_called_once_with()
    assert got is sentinel


def test_query_result_contract_fields():
    assert [f.name for f in fields(QueryResult)] == [
        "columns",
        "rows",
        "row_count",
        "result_id",
        "query_run_id",
        "execution_time_ms",
        "warning",
        "error_message",
    ]
