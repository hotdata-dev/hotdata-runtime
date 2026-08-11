from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from hotdata.exceptions import ApiException
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl

from hotdata_framework.client import HotdataClient
from hotdata_framework.databases import (
    ManagedDatabase,
    is_parquet_path,
    managed_database_from_detail,
)

_MANAGED_DB = ManagedDatabase(id="db_1", description="d", default_connection_id="conn_1")


def _decl_key_supported() -> bool:
    # `key` ships with the regenerated client; the key tests activate once it does.
    try:
        return DatabaseDefaultTableDecl(name="t", key=["k"]).key == ["k"]
    except Exception:
        return False


requires_key_field = pytest.mark.skipif(
    not _decl_key_supported(),
    reason="hotdata client without `key` on managed-table decls",
)


def _client() -> HotdataClient:
    return HotdataClient("k", "ws", host="https://api.hotdata.dev")


def _detail(id="db_1", description="sales", default_connection_id="conn_1"):
    return SimpleNamespace(
        id=id,
        name=description,
        default_connection_id=default_connection_id,
    )


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/data/orders.parquet", True),
        ("/data/ORDERS.PARQUET", True),
        ("/data/orders.csv", False),
    ],
)
def test_is_parquet_path(path: str, expected: bool):
    assert is_parquet_path(path) is expected


def test_managed_database_from_detail():
    db = managed_database_from_detail(_detail())
    assert db.id == "db_1"
    assert db.description == "sales"
    assert db.default_connection_id == "conn_1"


def test_list_managed_databases_returns_all():
    client = _client()
    summary = SimpleNamespace(id="db_1")
    detail = _detail()
    listing = SimpleNamespace(databases=[summary])
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.list_databases.return_value = listing
        dbs.return_value.get_database.return_value = detail
        result = client.list_managed_databases()
    assert len(result) == 1
    assert result[0].id == "db_1"
    assert result[0].description == "sales"


def test_list_managed_databases_skips_failed_gets():
    client = _client()
    summaries = [SimpleNamespace(id="db_1"), SimpleNamespace(id="db_2")]
    detail = _detail(id="db_2", description="warehouse", default_connection_id="conn_2")
    listing = SimpleNamespace(databases=summaries)
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.list_databases.return_value = listing
        dbs.return_value.get_database.side_effect = [
            ApiException(status=404, reason="not found"),
            detail,
        ]
        result = client.list_managed_databases()
    assert len(result) == 1
    assert result[0].id == "db_2"


def test_resolve_managed_database_by_id():
    client = _client()
    detail = _detail()
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.get_database.return_value = detail
        db = client.resolve_managed_database("db_1")
    assert db.id == "db_1"
    assert db.default_connection_id == "conn_1"


def test_resolve_managed_database_by_description():
    client = _client()
    summary = SimpleNamespace(id="db_1", name="sales")
    listing = SimpleNamespace(databases=[summary])
    detail = _detail()
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.get_database.side_effect = [
            ApiException(status=404, reason="not found"),
            detail,
        ]
        dbs.return_value.list_databases.return_value = listing
        db = client.resolve_managed_database("sales")
    assert db.id == "db_1"


def test_resolve_managed_database_not_found():
    client = _client()
    listing = SimpleNamespace(databases=[])
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.get_database.side_effect = ApiException(status=404, reason="not found")
        dbs.return_value.list_databases.return_value = listing
        with pytest.raises(KeyError, match="no-such"):
            client.resolve_managed_database("no-such")


def test_create_managed_database_returns_summary():
    client = _client()
    created = _detail(id="db_new", description="mydb", default_connection_id="conn_new")
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.create_database.return_value = created
        db = client.create_managed_database("mydb", tables=["orders"])
    assert db.id == "db_new"
    assert db.description == "mydb"
    req = dbs.return_value.create_database.call_args.args[0]
    assert req.schemas[0].tables[0].name == "orders"


def test_create_managed_database_wraps_api_errors():
    client = _client()
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.create_database.side_effect = ApiException(
            status=400,
            reason="bad request",
        )
        with pytest.raises(RuntimeError, match="bad request"):
            client.create_managed_database("mydb")


def test_create_managed_database_with_expires_at():
    client = _client()
    created = _detail()
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.create_database.return_value = created
        client.create_managed_database("mydb", expires_at="7d")
    req = dbs.return_value.create_database.call_args.args[0]
    assert req.expires_at == "7d"


def test_delete_managed_database_calls_sdk():
    client = _client()
    db = managed_database_from_detail(_detail())
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "_databases_api") as dbs,
    ):
        client.delete_managed_database("db_1")
    dbs.return_value.delete_database.assert_called_once_with("db_1")


def test_list_managed_tables_builds_full_names():
    client = _client()
    db = managed_database_from_detail(_detail())
    table = SimpleNamespace(
        connection="conn_1",
        var_schema="public",
        table="orders",
        synced=True,
        last_sync="2026-05-19T00:00:00Z",
    )
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "iter_tables", return_value=[table]),
    ):
        rows = client.list_managed_tables("db_1")
    assert len(rows) == 1
    assert rows[0].full_name == "db_1.public.orders"
    assert rows[0].synced is True


def test_upload_parquet_rejects_non_parquet():
    client = _client()
    with pytest.raises(ValueError, match="parquet"):
        client.upload_parquet("/tmp/data.csv")


def test_upload_parquet_delegates_to_sdk_upload_file():
    client = _client()
    finalized = SimpleNamespace(upload_id="upl_final")

    with patch.object(client, "uploads") as uploads:
        uploads.return_value.upload_file.return_value = finalized
        upload_id = client.upload_parquet("/tmp/data.parquet")

    assert upload_id == "upl_final"
    uploads.return_value.upload_file.assert_called_once_with(
        "/tmp/data.parquet", content_type="application/octet-stream"
    )


def test_upload_parquet_surfaces_api_cause_from_upload_error():
    # SessionCreateError et al. wrap the ApiException as __cause__; the wrapper
    # must re-raise with that ApiException as the DIRECT cause so
    # classify_sdk_error keeps seeing the status code.
    from hotdata.uploads import SessionCreateError

    client = _client()
    api_exc = ApiException(status=501)
    sdk_err = SessionCreateError("opening the upload session failed")
    sdk_err.__cause__ = api_exc

    with patch.object(client, "uploads") as uploads:
        uploads.return_value.upload_file.side_effect = sdk_err
        with pytest.raises(RuntimeError) as exc_info:
            client.upload_parquet("/tmp/data.parquet")

    assert exc_info.value.__cause__ is api_exc


def test_upload_parquet_wraps_plain_upload_error():
    from hotdata.uploads import StorageError

    client = _client()
    sdk_err = StorageError(status=503, part_number=3, body="upstream unavailable")

    with patch.object(client, "uploads") as uploads:
        uploads.return_value.upload_file.side_effect = sdk_err
        with pytest.raises(RuntimeError, match="503"):
            client.upload_parquet("/tmp/data.parquet")


def test_classify_501_is_terminal():
    from hotdata_framework.errors import (
        HotdataTerminalError,
        HotdataTransientError,
        classify_sdk_error,
    )

    assert isinstance(classify_sdk_error(ApiException(status=501)), HotdataTerminalError)
    # The rest of 5xx stays transient.
    assert isinstance(classify_sdk_error(ApiException(status=503)), HotdataTransientError)


def test_load_managed_table_with_upload_id():
    client = _client()
    db = managed_database_from_detail(_detail())
    loaded = SimpleNamespace(
        connection_id="conn_1",
        schema_name="public",
        table_name="orders",
        row_count=42,
    )
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "connections") as connections,
    ):
        connections.return_value.load_managed_table.return_value = loaded
        result = client.load_managed_table(
            "db_1",
            "orders",
            upload_id="upl_123",
        )
    assert result.row_count == 42
    assert result.full_name == "db_1.public.orders"
    connections.return_value.load_managed_table.assert_called_once_with(
        "conn_1", "public", "orders", _Any()
    )


def test_load_managed_table_requires_exactly_one_source():
    client = _client()
    with pytest.raises(ValueError, match="Exactly one"):
        client.load_managed_table("db_1", "orders")
    with pytest.raises(ValueError, match="Exactly one"):
        client.load_managed_table(
            "db_1",
            "orders",
            upload_id="upl_1",
            file="/tmp/x.parquet",
        )


def _load_and_capture_request(client, **kwargs):
    db = managed_database_from_detail(_detail())
    loaded = SimpleNamespace(
        connection_id="conn_1", schema_name="public", table_name="orders", row_count=1
    )
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "connections") as connections,
    ):
        connections.return_value.load_managed_table.return_value = loaded
        client.load_managed_table("db_1", "orders", upload_id="upl_1", **kwargs)
    return connections.return_value.load_managed_table.call_args.args[3]


def test_load_managed_table_defaults_to_replace():
    assert _load_and_capture_request(_client()).mode == "replace"


def test_load_managed_table_passes_key():
    request = _load_and_capture_request(_client(), mode="delete", key=["id"])
    assert request.mode == "delete"
    assert request.key == ["id"]


def test_load_managed_table_key_defaults_to_none():
    assert _load_and_capture_request(_client()).key is None


@pytest.mark.parametrize("mode", ["append", "delete", "update", "upsert"])
def test_load_managed_table_forwards_mode(mode: str):
    assert _load_and_capture_request(_client(), mode=mode).mode == mode


@requires_key_field
def test_create_managed_database_declares_keys():
    client = _client()
    with patch.object(client, "_databases_api") as dbs:
        dbs.return_value.create_database.return_value = _detail(id="db_new")
        client.create_managed_database(
            "mydb", tables=["orders", "events"], keys={"orders": ["id"]}
        )
    req = dbs.return_value.create_database.call_args.args[0]
    declared = {t.name: list(t.key) for t in req.schemas[0].tables}
    assert declared == {"orders": ["id"], "events": []}


@requires_key_field
def test_add_managed_table_declares_key():
    client = _client()
    db = managed_database_from_detail(_detail())
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "_databases_api") as dbs,
    ):
        client.add_managed_table("db_1", "line_items", key=["order_id", "sku"])
    req = dbs.return_value.add_database_table.call_args.args[2]
    assert req.name == "line_items"
    assert list(req.key) == ["order_id", "sku"]


@requires_key_field
def test_add_managed_table_keyless_by_default():
    client = _client()
    db = managed_database_from_detail(_detail())
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "_databases_api") as dbs,
    ):
        client.add_managed_table("db_1", "orders")
    req = dbs.return_value.add_database_table.call_args.args[2]
    assert list(req.key) == []


def test_delete_managed_table_uses_default_connection_id():
    client = _client()
    db = managed_database_from_detail(_detail())
    with (
        patch.object(client, "resolve_managed_database", return_value=db),
        patch.object(client, "connections") as connections,
    ):
        client.delete_managed_table("db_1", "orders")
    connections.return_value.delete_managed_table.assert_called_once_with(
        "conn_1",
        "public",
        "orders",
    )


class _Any:
    """Matches any value in assert_called_once_with."""

    def __eq__(self, other: object) -> bool:
        return True


# ---------------------------------------------------------------------------
# Table layout: declaring it, and reading it back
#
# A layout is fixed when the table is created and there is no alter path, so a
# silently dropped argument produces a table that can never be repaired — only
# recreated with its data rewritten. That is why these assert on the REQUEST
# object reaching the API rather than on the call succeeding: the generated model
# ignores unknown fields, so passing `partition_by=` to a client whose model
# lacks it returns 201 and declares a layout-less table. That exact silent drop
# is why this package could not express a layout until now.


def _layout():
    from hotdata_framework import TablePartitionKey, TableSortKey

    return (
        [TablePartitionKey(column="event_date", transform="identity")],
        [TableSortKey(column="event_time", direction="desc", nulls="last")],
    )


def test_add_managed_table_sends_the_layout():

    parts, sorts = _layout()
    client = _client()
    captured = {}

    class FakeDatabasesApi:
        def add_database_table(self, db_id, schema, request):
            captured["request"] = request
            captured["schema"] = schema

    with patch.object(client, "_databases_api", return_value=FakeDatabasesApi()), patch.object(
        client, "_as_managed_database", return_value=_MANAGED_DB
    ):
        client.add_managed_table("db_1", "files", key=["id"], partition_by=parts, sorted_by=sorts)

    req = captured["request"]
    # Serialised, because that is what actually goes on the wire — a field the
    # model does not know about vanishes here rather than at the call site.
    body = req.to_dict()
    assert body["partition_by"] == [{"column": "event_date", "transform": "identity"}]
    assert body["sorted_by"] == [{"column": "event_time", "direction": "desc", "nulls": "last"}]
    assert body["key"] == ["id"]


def test_add_managed_table_omits_layout_when_not_asked():
    """Omitted must stay omitted: sending empty arrays would declare "explicitly
    no layout" on a server that distinguishes absent from empty."""

    client = _client()
    captured = {}

    class FakeDatabasesApi:
        def add_database_table(self, db_id, schema, request):
            captured["request"] = request

    with patch.object(client, "_databases_api", return_value=FakeDatabasesApi()), patch.object(
        client, "_as_managed_database", return_value=_MANAGED_DB
    ):
        client.add_managed_table("db_1", "files")

    body = captured["request"].to_dict()
    assert body.get("partition_by") is None
    assert body.get("sorted_by") is None


def test_create_managed_database_sends_per_table_layout():

    parts, sorts = _layout()
    client = _client()
    captured = {}

    class FakeDatabasesApi:
        def create_database(self, request):
            captured["request"] = request
            return _detail(id="db_new", description="demo")

    with patch.object(client, "_databases_api", return_value=FakeDatabasesApi()):
        client.create_managed_database(
            "demo",
            tables=["files", "other"],
            keys={"files": ["id"]},
            partition_by={"files": parts},
            sorted_by={"files": sorts},
        )

    decls = captured["request"].to_dict()["schemas"][0]["tables"]
    by_name = {d["name"]: d for d in decls}
    assert by_name["files"]["partition_by"] == [
        {"column": "event_date", "transform": "identity"}
    ]
    assert by_name["files"]["sorted_by"] == [
        {"column": "event_time", "direction": "desc", "nulls": "last"}
    ]
    # A table absent from the mapping gets no layout, not an empty one.
    assert by_name["other"].get("partition_by") is None
    assert by_name["other"].get("sorted_by") is None


def test_managed_table_layout_reads_it_back():

    parts, sorts = _layout()
    client = _client()
    info = SimpleNamespace(
        table="files", var_schema="public", partition_by=parts, sorted_by=sorts
    )
    other = SimpleNamespace(table="elsewhere", var_schema="public", partition_by=[], sorted_by=[])

    with patch.object(client, "iter_tables", return_value=iter([other, info])), patch.object(
        client, "_as_managed_database", return_value=_MANAGED_DB
    ):
        layout = client.managed_table_layout("db_1", "files")

    assert layout.table_name == "files"
    assert [p.column for p in layout.partition_by] == ["event_date"]
    assert [s.column for s in layout.sorted_by] == ["event_time"]
    assert layout.is_partitioned is True
    assert layout.is_sorted is True


def test_managed_table_layout_distinguishes_absent_from_unpartitioned():
    """KeyError for a missing table, empty lists for one declared without a
    layout. Collapsing the two would let a caller treat "table isn't there" as
    "confirmed no layout" and load into something it never verified."""

    client = _client()
    plain = SimpleNamespace(table="files", var_schema="public", partition_by=[], sorted_by=[])

    with patch.object(client, "iter_tables", return_value=iter([plain])), patch.object(
        client, "_as_managed_database", return_value=_MANAGED_DB
    ):
        layout = client.managed_table_layout("db_1", "files")
        assert layout.is_partitioned is False and layout.is_sorted is False

        with pytest.raises(KeyError, match="missing"):
            client.managed_table_layout("db_1", "missing")
