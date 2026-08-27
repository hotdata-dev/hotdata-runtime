from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from hotdata import Configuration
from hotdata.exceptions import ForbiddenException

from hotdata_framework.client import HotdataClient
from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.env import normalize_host, pick_workspace, resolve_workspace_selection


class _ForbiddenDatabasesApi:
    """A `/databases` API that a create-scoped key would see: every read is 403,
    while the declare-table write succeeds. Counts reads so tests can assert none
    happened."""

    def __init__(self) -> None:
        self.read_calls = 0
        self.add_calls: list[tuple[str, str, str]] = []

    def get_database(self, database_id: str):
        self.read_calls += 1
        raise ForbiddenException(status=403)

    def list_databases(self):
        self.read_calls += 1
        raise ForbiddenException(status=403)

    def add_database_table(self, database_id, var_schema, request):
        self.add_calls.append((database_id, var_schema, request.name))
        return SimpleNamespace(
            connection_id="conn", var_schema=var_schema, table=request.name
        )


class _FakeConnectionsApi:
    def __init__(self, responses=None) -> None:
        self.load_calls: list[tuple[str, str, str]] = []
        self.requests: list = []
        self._responses = list(responses) if responses else None

    def load_managed_table(self, connection_id, schema, table, request):
        self.load_calls.append((connection_id, schema, table))
        self.requests.append(request)
        if self._responses:
            return self._responses.pop(0)
        return SimpleNamespace(
            connection_id=connection_id,
            schema_name=schema,
            table_name=table,
            row_count=3,
        )


def test_load_managed_table_with_object_skips_read_probe():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    databases = _ForbiddenDatabasesApi()
    connections = _FakeConnectionsApi()

    with (
        patch.object(client, "_databases_api", return_value=databases),
        patch.object(client, "connections", return_value=connections),
    ):
        result = client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    assert databases.read_calls == 0
    assert connections.load_calls == [("conn_1", "public", "orders")]
    assert result.full_name == "db_1.public.orders"
    assert result.row_count == 3


def test_add_managed_table_with_object_skips_read_probe():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    databases = _ForbiddenDatabasesApi()

    with patch.object(client, "_databases_api", return_value=databases):
        result = client.add_managed_table(db, "orders", schema="public")

    assert databases.read_calls == 0
    assert databases.add_calls == [("db_1", "public", "orders")]
    assert result.full_name == "db_1.public.orders"


def test_execute_sql_with_object_skips_read_probe():
    from hotdata.models.query_response import QueryResponse as _QR

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_abc", description="mydb", default_connection_id="conn_1")
    databases = _ForbiddenDatabasesApi()

    class FakeQueryApi:
        def __init__(self):
            self.calls: list[dict] = []

        def query(self, request, **kwargs):
            self.calls.append(kwargs)
            return _QR(
                columns=["n"],
                rows=[[1]],
                row_count=1,
                preview_row_count=1,
                truncated=False,
                nullable=[False],
                result_id="res_1",
                query_run_id="qrun_1",
                execution_time_ms=1,
            )

    fake_q = FakeQueryApi()
    with (
        patch.object(client, "_query_api", return_value=fake_q),
        patch.object(client, "_databases_api", return_value=databases),
    ):
        client.execute_sql("SELECT 1", database=db)

    assert databases.read_calls == 0
    assert fake_q.calls == [{"x_database_id": "db_abc"}]


def test_load_managed_table_with_name_still_resolves():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    connections = _FakeConnectionsApi()
    resolved = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")

    with (
        patch.object(client, "resolve_managed_database", return_value=resolved) as resolve,
        patch.object(client, "connections", return_value=connections),
    ):
        result = client.load_managed_table("mydb", "orders", schema="public", upload_id="up_1")

    resolve.assert_called_once_with("mydb")
    assert connections.load_calls == [("conn_1", "public", "orders")]
    assert result.full_name == "db_1.public.orders"


def _clear_workspace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HOTDATA_WORKSPACE", raising=False)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://api.hotdata.dev", "https://api.hotdata.dev"),
        ("https://api.hotdata.dev/", "https://api.hotdata.dev"),
        ("https://api.hotdata.dev/v1", "https://api.hotdata.dev"),
        ("https://api.hotdata.dev/v1/", "https://api.hotdata.dev"),
        ("http://localhost:8000/v1", "http://localhost:8000"),
        ("http://localhost:8000", "http://localhost:8000"),
    ],
)
def test_normalize_host(raw: str, expected: str):
    assert normalize_host(raw) == expected


def test_pick_workspace_prefers_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_explicit")
    assert pick_workspace("k", "https://api.hotdata.dev") == "ws_explicit"


def test_resolve_workspace_selection_prefers_env_without_listing(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_explicit")
    with patch("hotdata_framework.env.list_workspaces") as listing:
        resolved = resolve_workspace_selection("k", "https://api.hotdata.dev")
    listing.assert_not_called()
    assert resolved.workspace_id == "ws_explicit"
    assert resolved.source == "explicit_env"
    assert resolved.workspaces == []


def test_pick_workspace_chooses_first_active(monkeypatch: pytest.MonkeyPatch):
    _clear_workspace_env(monkeypatch)

    items = [
        SimpleNamespace(public_id="ws_1", active=False),
        SimpleNamespace(public_id="ws_2", active=True),
        SimpleNamespace(public_id="ws_3", active=True),
    ]
    listing = SimpleNamespace(workspaces=items)

    with patch("hotdata_framework.env.WorkspacesApi") as Api:
        Api.return_value.list_workspaces.return_value = listing
        assert pick_workspace("k", "https://api.hotdata.dev") == "ws_2"


def test_pick_workspace_falls_back_to_first(monkeypatch: pytest.MonkeyPatch):
    _clear_workspace_env(monkeypatch)

    items = [
        SimpleNamespace(public_id="ws_1", active=False),
        SimpleNamespace(public_id="ws_2", active=False),
    ]
    listing = SimpleNamespace(workspaces=items)

    with patch("hotdata_framework.env.WorkspacesApi") as Api:
        Api.return_value.list_workspaces.return_value = listing
        assert pick_workspace("k", "https://api.hotdata.dev") == "ws_1"


def test_resolve_workspace_selection_source_first(monkeypatch: pytest.MonkeyPatch):
    _clear_workspace_env(monkeypatch)
    items = [
        SimpleNamespace(public_id="ws_1", active=False),
        SimpleNamespace(public_id="ws_2", active=False),
    ]
    listing = SimpleNamespace(workspaces=items)
    with patch("hotdata_framework.env.WorkspacesApi") as Api:
        Api.return_value.list_workspaces.return_value = listing
        resolved = resolve_workspace_selection("k", "https://api.hotdata.dev")
    assert resolved.workspace_id == "ws_1"
    assert resolved.source == "first"
    assert resolved.workspaces == items


def test_resolve_workspace_selection_returns_workspaces_and_source(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_workspace_env(monkeypatch)

    items = [
        SimpleNamespace(public_id="ws_1", active=False),
        SimpleNamespace(public_id="ws_2", active=True),
    ]
    listing = SimpleNamespace(workspaces=items)

    with patch("hotdata_framework.env.WorkspacesApi") as Api:
        Api.return_value.list_workspaces.return_value = listing
        resolved = resolve_workspace_selection("k", "https://api.hotdata.dev")
    assert resolved.workspace_id == "ws_2"
    assert resolved.source == "active"
    assert resolved.workspaces == items


def test_list_qualified_table_names_passes_connection_id():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    with patch.object(client, "iter_tables", return_value=iter([])) as it:
        client.list_qualified_table_names(limit=5, connection_id="conn_a")
    it.assert_called_once()
    assert it.call_args.kwargs["connection_id"] == "conn_a"


def test_wait_result_ready_raises_on_cancelled():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class FakeResultsApi:
        def get_result(self, result_id: str):
            return SimpleNamespace(status="cancelled", error_message=None)

    with (
        patch.object(client, "_results_api", return_value=FakeResultsApi()),
        pytest.raises(RuntimeError, match="cancelled"),
    ):
        client._wait_result_ready("res_1", timeout_s=0.1, interval_s=0)


def test_connection_id_by_name_raises_on_duplicate_names():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    listing = SimpleNamespace(
        connections=[
            SimpleNamespace(name="warehouse", id="conn_1"),
            SimpleNamespace(name="warehouse", id="conn_2"),
        ]
    )

    class FakeConnectionsApi:
        def list_connections(self):
            return listing

    with (
        patch.object(client, "connections", return_value=FakeConnectionsApi()),
        pytest.raises(RuntimeError, match="Duplicate connection names"),
    ):
        client.connection_id_by_name()


def test_columns_for_qualified_prefers_explicit_connection_id():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    col = SimpleNamespace(name="a", data_type="INTEGER", nullable=True)
    table = SimpleNamespace(columns=[col])
    response = SimpleNamespace(tables=[table])

    class FakeInformationSchemaApi:
        def __init__(self):
            self.kwargs = None

        def information_schema(self, **kwargs):
            self.kwargs = kwargs
            return response

    fake_api = FakeInformationSchemaApi()
    with (
        patch.object(client, "_information_schema", return_value=fake_api),
        patch.object(client, "connection_id_by_name") as id_map,
    ):
        cols = client.columns_for_qualified(
            "warehouse.public.orders",
            connection_id="conn_explicit",
        )
    id_map.assert_not_called()
    assert cols == [col]
    assert fake_api.kwargs["connection_id"] == "conn_explicit"


def test_add_managed_table_declares_table_on_existing_database():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    fake_db = SimpleNamespace(id="db_1", default_connection_id="conn")

    class FakeDatabasesApi:
        def __init__(self):
            self.calls: list[tuple[str, str, str]] = []

        def add_database_table(self, database_id, var_schema, request):
            self.calls.append((database_id, var_schema, request.name))
            return SimpleNamespace(
                connection_id="conn", var_schema=var_schema, table=request.name
            )

    fake_api = FakeDatabasesApi()
    with (
        patch.object(client, "resolve_managed_database", return_value=fake_db),
        patch.object(client, "_databases_api", return_value=fake_api),
    ):
        result = client.add_managed_table("mydb", "orders", schema="public")

    assert fake_api.calls == [("db_1", "public", "orders")]
    assert result.full_name == "db_1.public.orders"
    assert result.schema == "public"
    assert result.table == "orders"
    assert result.synced is False
    assert result.last_sync is None


def test_list_recent_results_returns_normalized_summaries():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    listing = SimpleNamespace(
        results=[
            SimpleNamespace(id="res_1", status="ready", created_at="2026-01-01T00:00:00Z"),
            SimpleNamespace(id="res_2", status="failed", created_at=None),
        ]
    )

    class FakeResultsApi:
        def list_results(self, *, limit: int, offset: int):
            return listing

    with patch.object(client, "results", return_value=FakeResultsApi()):
        out = client.list_recent_results(limit=10, offset=2)
    assert [r.result_id for r in out] == ["res_1", "res_2"]
    assert out[0].status == "ready"
    assert out[0].to_dict()["created_at"] == "2026-01-01T00:00:00Z"


def test_execute_sql_sends_no_database_id_by_default():
    from hotdata.models.query_response import QueryResponse as _QR

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class FakeQueryApi:
        def __init__(self):
            self.calls: list[dict] = []

        def query(self, request, **kwargs):
            self.calls.append(kwargs)
            return _QR(
                columns=["n"],
                rows=[[1]],
                row_count=1,
                preview_row_count=1,
                truncated=False,
                nullable=[False],
                result_id="res_1",
                query_run_id="qrun_1",
                execution_time_ms=1,
            )

    fake_q = FakeQueryApi()
    with patch.object(client, "_query_api", return_value=fake_q):
        client.execute_sql("SELECT 1")

    assert fake_q.calls == [{}]


def test_execute_sql_resolves_database_and_sends_x_database_id():
    from types import SimpleNamespace

    from hotdata.models.query_response import QueryResponse as _QR

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class FakeQueryApi:
        def __init__(self):
            self.calls: list[dict] = []

        def query(self, request, **kwargs):
            self.calls.append(kwargs)
            return _QR(
                columns=["n"],
                rows=[[1]],
                row_count=1,
                preview_row_count=1,
                truncated=False,
                nullable=[False],
                result_id="res_1",
                query_run_id="qrun_1",
                execution_time_ms=1,
            )

    fake_q = FakeQueryApi()
    fake_db = SimpleNamespace(id="db_abc")

    with (
        patch.object(client, "_query_api", return_value=fake_q),
        patch.object(client, "resolve_managed_database", return_value=fake_db) as resolve,
    ):
        client.execute_sql('SELECT * FROM "default"."public"."orders"', database="my_db")

    resolve.assert_called_once_with("my_db")
    assert fake_q.calls == [{"x_database_id": "db_abc"}]


def test_list_run_history_returns_normalized_items():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    listing = SimpleNamespace(
        query_runs=[
            SimpleNamespace(
                id="run_1",
                status="succeeded",
                created_at="2026-01-01T00:00:00Z",
                execution_time_ms=7,
                result_id="res_1",
            ),
        ]
    )

    class FakeRunsApi:
        def __init__(self):
            self.kwargs = None

        def list_query_runs(self, *, limit: int):
            self.kwargs = {"limit": limit}
            return listing

    fake_runs = FakeRunsApi()
    with patch.object(client, "query_runs", return_value=fake_runs):
        out = client.list_run_history(limit=5)
    assert [r.query_run_id for r in out] == ["run_1"]
    assert out[0].execution_time_ms == 7
    assert out[0].to_dict()["result_id"] == "res_1"
    assert fake_runs.kwargs == {"limit": 5}


# ---------------------------------------------------------------------------
# Session removal — asserted at the wire, not at the signature
#
# A signature-shaped check ("does __init__ accept session_id?") is satisfied by
# any unrecognised keyword and says nothing about what goes on the request. The
# feature was proven revivable from the ENVIRONMENT with the whole suite green:
# read HOTDATA_SANDBOX, pass it to Configuration, and the X-Session-Id header is
# back on every call with no signature change to notice. So these assert on
# `Configuration.api_keys`, which is where the generated client actually decides
# to send the header, and they set HOTDATA_SANDBOX so an environment-sourced
# revival has something to find.


def _sandbox_set(monkeypatch: pytest.MonkeyPatch) -> str:
    value = "sb_should_reach_nothing"
    monkeypatch.setenv("HOTDATA_SANDBOX", value)
    return value


def test_client_registers_no_session_header(monkeypatch: pytest.MonkeyPatch):
    """The removal's observable contract: no X-Session-Id on anything this client
    sends. `api_keys` is the SDK's own record of which security schemes it will
    attach, so an empty SessionId slot is the header being absent."""
    _sandbox_set(monkeypatch)
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    assert "SessionId" not in client.api.configuration.api_keys
    assert client.api.configuration.api_keys == {"WorkspaceId": "ws"}


def test_client_rejects_session_id_by_name(monkeypatch: pytest.MonkeyPatch):
    """`match=` matters: without it the assertion passes for ANY unknown keyword,
    making it a typo detector rather than a statement about `session_id`."""
    _sandbox_set(monkeypatch)
    with pytest.raises(TypeError, match="session_id"):
        HotdataClient("k", "ws", host="https://api.hotdata.dev", session_id="sb_x")
    # The property is gone too — the CHANGELOG says so, and an adapter reading it
    # should get AttributeError rather than a stale value.
    assert not hasattr(HotdataClient("k", "ws", host="https://api.hotdata.dev"), "session_id")


def test_workspace_listing_registers_no_session_header(monkeypatch: pytest.MonkeyPatch):
    """`list_workspaces` builds its OWN Configuration, so it is a second place the
    header can come back — and it is the pre-auth call, made before any workspace
    is known."""
    _clear_workspace_env(monkeypatch)
    _sandbox_set(monkeypatch)
    seen: list[Configuration] = []

    def spy(*args: object, **kwargs: object) -> Configuration:
        # Assert on the CONFIG, not on the kwargs. `session_id=` is only one way
        # back: `Configuration(api_keys={"SessionId": ...})` is a documented
        # escape hatch that re-attaches the header with no such keyword, and a
        # kwargs-only check waves it through.
        cfg = Configuration(*args, **kwargs)
        seen.append(cfg)
        return cfg

    listing = SimpleNamespace(workspaces=[SimpleNamespace(public_id="ws_1", active=True)])
    with patch("hotdata_framework.env.Configuration", spy), patch(
        "hotdata_framework.env.WorkspacesApi"
    ) as Api:
        Api.return_value.list_workspaces.return_value = listing
        assert pick_workspace("k", "https://api.hotdata.dev") == "ws_1"
    assert seen, "list_workspaces built no Configuration — the spy never fired"
    for cfg in seen:
        assert "SessionId" not in cfg.api_keys


def test_from_env_builds_a_client_without_a_session(monkeypatch: pytest.MonkeyPatch):
    """`from_env` is the entry point every adapter uses, and its body had no
    coverage at all: the module-level `from_env` test patches this classmethod
    out. Leaving a stale 3-argument `pick_workspace(...)` call here broke it
    unconditionally while the suite stayed green."""
    _clear_workspace_env(monkeypatch)
    _sandbox_set(monkeypatch)
    monkeypatch.setenv("HOTDATA_API_KEY", "k_env")
    monkeypatch.setenv("HOTDATA_API_URL", "https://api.hotdata.dev")

    with patch("hotdata_framework.client.pick_workspace") as picked:
        picked.return_value = "ws_from_env"
        client = HotdataClient.from_env()

    # Two arguments, positionally — the signature this change trimmed.
    assert picked.call_args.args == ("k_env", "https://api.hotdata.dev")
    assert picked.call_args.kwargs == {}
    assert client.workspace_id == "ws_from_env"
    assert client.host == "https://api.hotdata.dev"
    assert "SessionId" not in client.api.configuration.api_keys


def test_from_env_requires_an_api_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("HOTDATA_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="HOTDATA_API_KEY"):
        HotdataClient.from_env()


# --------------------------------------------------------------------------
# A load is submitted as a job, not held open on one request
# --------------------------------------------------------------------------


def _load_response(rows=7):
    from hotdata.models.load_managed_table_response import LoadManagedTableResponse

    return LoadManagedTableResponse(
        connection_id="conn_1", schema_name="public",
        table_name="orders", row_count=rows,
        arrow_schema_json="{}",
    )


def test_a_load_asks_for_a_job_with_an_inline_window():
    """The request itself is the fix. A load whose duration scales with the data
    must not depend on one HTTP request surviving minutes through every layer
    between here and the engine; when such a request dies the server discards the
    work and leaves the table locked against the retry."""
    from hotdata_framework.client import _LOAD_INLINE_WAIT_MS

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi()

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
    ):
        client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    req = connections.requests[0]
    assert req.var_async is True, "load was not submitted as a job"
    assert req.async_after_ms == _LOAD_INLINE_WAIT_MS


def test_a_load_that_finishes_inline_costs_no_polling():
    """dlt's bookkeeping tables settle in under a second. Making those pay a
    submit-then-poll round trip would be a regression, which is what
    `async_after_ms` exists to prevent."""
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi()

    def _forbidden_poll(*a, **k):
        raise AssertionError("polled a load the server answered inline")

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", _forbidden_poll),
    ):
        result = client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    assert result.row_count == 3


def test_a_load_the_server_defers_is_polled_to_completion():
    """The 202 path: the result comes from durable job state rather than from a
    connection that had to stay alive to carry it."""
    from hotdata.models.submit_job_response import SubmitJobResponse

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_1", status="running", status_url="/v1/jobs/jobs_1"),
    ])
    final = SimpleNamespace(
        status="succeeded", error_message=None,
        result=SimpleNamespace(actual_instance=_load_response(rows=91)),
    )

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final) as poll,
    ):
        result = client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    assert result.row_count == 91
    assert poll.call_args.args[0] == "jobs_1"


def test_a_load_job_gets_its_own_budget_not_the_query_one():
    """Reusing the 300s query budget is what put a five-minute ceiling on loads in
    the first place; a load is the one operation whose duration scales with the
    data."""
    from hotdata.models.submit_job_response import SubmitJobResponse

    from hotdata_framework.client import _LOAD_JOB_TIMEOUT_S

    assert _LOAD_JOB_TIMEOUT_S > 300.0

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_1", status="running", status_url="/v1/jobs/jobs_1"),
    ])
    final = SimpleNamespace(
        status="succeeded", error_message=None,
        result=SimpleNamespace(actual_instance=_load_response()),
    )

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final) as poll,
    ):
        client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    assert poll.call_args.kwargs["timeout_s"] == _LOAD_JOB_TIMEOUT_S


def test_a_failed_load_job_raises_with_the_server_message():
    from hotdata.models.submit_job_response import SubmitJobResponse

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_1", status="running", status_url="/v1/jobs/jobs_1"),
    ])
    final = SimpleNamespace(status="failed", error_message="disk full", result=None)

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final),
    ):
        try:
            client.load_managed_table(db, "orders", schema="public", upload_id="up_1")
        except RuntimeError as e:
            assert "disk full" in str(e), e
        else:
            raise AssertionError("a failed load job did not raise")


def test_a_partially_succeeded_load_job_is_not_treated_as_success():
    """A caller asked for a table's contents to be replaced or appended to;
    "some of it" is not an answer it can use."""
    from hotdata.models.submit_job_response import SubmitJobResponse

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_1", status="running", status_url="/v1/jobs/jobs_1"),
    ])
    final = SimpleNamespace(
        status="partially_succeeded", error_message="3 rows rejected",
        result=SimpleNamespace(actual_instance=_load_response()),
    )

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final),
    ):
        try:
            client.load_managed_table(db, "orders", schema="public", upload_id="up_1")
        except RuntimeError as e:
            assert "3 rows rejected" in str(e), e
        else:
            raise AssertionError("partially_succeeded was treated as success")


def test_a_transient_status_check_does_not_discard_a_running_job():
    """The regression this guards. A load polls for up to an hour, so there are
    hundreds of status checks; aborting on the first bad one throws away a job
    that is still running, and the caller's retry then re-submits the load while
    the original still holds the table -- the 409 spin, from a new direction."""
    from hotdata.exceptions import ApiException

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    calls = {"n": 0}
    final = SimpleNamespace(status="succeeded", error_message=None,
                            result=SimpleNamespace(actual_instance=_load_response(5)))

    class _Flaky:
        def get_job(self, job_id):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ApiException(status=502, reason="Bad Gateway")
            return final

    with (
        patch.object(client, "_jobs_api", return_value=_Flaky()),
        patch("time.sleep", lambda *_: None),
    ):
        got = client._poll_job("jobs_1", timeout_s=60.0, interval_s=0.01)

    assert got is final, "a blipping status check aborted the poll"
    assert calls["n"] == 3


def test_a_sustained_run_of_failed_status_checks_gives_up_naming_the_job():
    """Tolerance is for blips, not for an API that has gone away -- and the message
    has to name the job, because this is one of the two paths where the caller
    cannot tell whether the load landed. `502: Bad Gateway` alone does not."""
    from hotdata.exceptions import ApiException

    from hotdata_framework.client import _JOB_POLL_ERROR_GRACE_S

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class _Dead:
        def get_job(self, job_id):
            raise ApiException(status=502, reason="Bad Gateway")

    # A FAKE CLOCK, advanced by the sleeps. The tolerance is measured in time, so
    # a no-op `sleep` with a real clock would make this test wait out the whole
    # grace period in wall time -- 120 seconds of a spinning loop to assert one
    # message.
    clock = {"t": 0.0}
    with (
        patch.object(client, "_jobs_api", return_value=_Dead()),
        patch("time.monotonic", lambda: clock["t"]),
        patch("time.sleep", lambda s: clock.__setitem__("t", clock["t"] + s)),
    ):
        try:
            client._poll_job("jobs_1", timeout_s=6000.0, interval_s=1.0)
        except RuntimeError as e:
            assert "jobs_1" in str(e), f"gave up without naming the job: {e}"
        else:
            raise AssertionError("polled forever against a dead API")
    assert clock["t"] >= _JOB_POLL_ERROR_GRACE_S, "gave up before the grace elapsed"
    assert clock["t"] < 6000.0, "ran to the poll deadline instead of the grace"


def test_the_error_tolerance_is_measured_in_time_not_checks():
    """A count means whatever the caller's `interval_s` makes it, and callers
    differ. The thing being survived is a gateway blip, which lasts seconds to
    minutes regardless of how often we happen to ask."""
    from hotdata_framework.client import _JOB_POLL_ERROR_GRACE_S

    # long enough to outlast a rolling restart / LB reconverge, not seconds
    assert _JOB_POLL_ERROR_GRACE_S >= 60.0


def test_failed_status_checks_back_off_instead_of_hammering():
    """Whatever is serving 502s does not need the extra traffic."""
    from hotdata.exceptions import ApiException

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    sleeps: list[float] = []
    calls = {"n": 0}
    final = SimpleNamespace(status="succeeded", error_message=None,
                            result=SimpleNamespace(actual_instance=_load_response()))

    class _Flaky:
        def get_job(self, job_id):
            calls["n"] += 1
            if calls["n"] < 5:
                raise ApiException(status=502, reason="Bad Gateway")
            return final

    with (
        patch.object(client, "_jobs_api", return_value=_Flaky()),
        patch("time.sleep", lambda s: sleeps.append(s)),
    ):
        client._poll_job("jobs_1", timeout_s=600.0, interval_s=1.0)

    failed_waits = sleeps[:4]
    assert failed_waits == sorted(failed_waits), f"did not back off: {failed_waits}"
    assert failed_waits[-1] > failed_waits[0]


def test_a_failed_load_job_names_the_job_alongside_the_server_message():
    from hotdata.models.submit_job_response import SubmitJobResponse

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_42", status="running", status_url="/v1/jobs/jobs_42"),
    ])
    final = SimpleNamespace(status="failed", error_message="disk full", result=None)

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final),
    ):
        try:
            client.load_managed_table(db, "orders", schema="public", upload_id="up_1")
        except RuntimeError as e:
            assert "disk full" in str(e) and "jobs_42" in str(e), e
        else:
            raise AssertionError("a failed load job did not raise")


def test_a_deferred_load_returns_the_job_id_to_the_caller():
    """The id is the handle a caller has to answer "did it land?" after a lost
    response -- the same reason CreateIndexResult carries one."""
    from hotdata.models.submit_job_response import SubmitJobResponse

    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    connections = _FakeConnectionsApi(responses=[
        SubmitJobResponse(id="jobs_77", status="running", status_url="/v1/jobs/jobs_77"),
    ])
    final = SimpleNamespace(status="succeeded", error_message=None,
                            result=SimpleNamespace(actual_instance=_load_response()))

    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=connections),
        patch.object(client, "_poll_job", return_value=final),
    ):
        result = client.load_managed_table(db, "orders", schema="public", upload_id="up_1")

    assert result.job_id == "jobs_77"


def test_an_inline_load_carries_no_job_id():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    db = ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")
    with (
        patch.object(client, "_databases_api", return_value=_ForbiddenDatabasesApi()),
        patch.object(client, "connections", return_value=_FakeConnectionsApi()),
    ):
        result = client.load_managed_table(db, "orders", schema="public", upload_id="up_1")
    assert result.job_id is None

