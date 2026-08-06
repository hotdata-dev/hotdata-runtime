from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from hotdata.exceptions import ApiException, ForbiddenException
from hotdata.models.index_info_response import IndexInfoResponse
from hotdata.models.index_status import IndexStatus
from hotdata.models.job_status import JobStatus
from hotdata.models.submit_job_response import SubmitJobResponse

from hotdata_framework.client import HotdataClient
from hotdata_framework.databases import ManagedDatabase

_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _index_info(
    *,
    index_name: str = "ix_embedding",
    index_type: str = "vector",
    columns: list[str] | None = None,
    metric: str | None = "cosine",
    source_column: str | None = None,
    status: IndexStatus = IndexStatus.READY,
) -> IndexInfoResponse:
    return IndexInfoResponse(
        columns=columns if columns is not None else ["embedding"],
        created_at=_TS,
        index_name=index_name,
        index_type=index_type,
        metric=metric,
        source_column=source_column,
        status=status,
        updated_at=_TS,
    )


def _submitted(job_id: str = "job_1") -> SubmitJobResponse:
    return SubmitJobResponse(
        id=job_id,
        status=JobStatus.PENDING,
        status_url=f"/v1/jobs/{job_id}",
    )


def _job(status: JobStatus, *, error_message: str | None = None, result: object = None):
    return SimpleNamespace(status=status, error_message=error_message, result=result)


class FakeIndexesApi:
    """Records the create call and replays a canned response."""

    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[tuple[str, str, str]] = []
        self.requests: list[object] = []

    def create_index(self, connection_id, var_schema, table, create_index_request):
        self.calls.append((connection_id, var_schema, table))
        self.requests.append(create_index_request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class FakeJobsApi:
    """Serves a scripted sequence of job states, repeating the last one."""

    def __init__(self, states: list[object]) -> None:
        self.states = states
        self.calls = 0

    def get_job(self, job_id: str):
        self.calls += 1
        index = min(self.calls - 1, len(self.states) - 1)
        return self.states[index]


class _ForbiddenDatabasesApi:
    def __init__(self) -> None:
        self.read_calls = 0

    def get_database(self, database_id: str):
        self.read_calls += 1
        raise ForbiddenException(status=403)

    def list_databases(self):
        self.read_calls += 1
        raise ForbiddenException(status=403)


def _client() -> HotdataClient:
    return HotdataClient("k", "ws", host="https://api.hotdata.dev")


def _db() -> ManagedDatabase:
    return ManagedDatabase(id="db_1", description="mydb", default_connection_id="conn_1")


def test_create_vector_index_polls_job_and_returns_built_index():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi(
        [
            _job(JobStatus.RUNNING),
            _job(JobStatus.SUCCEEDED, result=SimpleNamespace(actual_instance=_index_info())),
        ]
    )

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_embedding",
            columns=["embedding"],
            index_type="vector",
            metric="cosine",
            poll_interval_s=0,
        )

    assert indexes.calls == [("conn_1", "public", "docs")]
    request = indexes.requests[0]
    assert request.index_name == "ix_embedding"
    assert request.columns == ["embedding"]
    assert request.index_type == "vector"
    assert request.metric == "cosine"
    # The async flag rides the "async" wire alias; without it the server would
    # build inline and the request could time out on a large table.
    assert request.var_async is True
    assert request.to_dict()["async"] is True

    assert jobs.calls == 2
    assert result.status == "ready"
    assert result.job_id == "job_1"
    assert result.index_name == "ix_embedding"
    assert result.index_type == "vector"
    assert result.metric == "cosine"
    assert result.columns == ["embedding"]
    assert result.full_name == "db_1.public.docs"
    assert result.schema_name == "public"
    assert result.table_name == "docs"


def test_create_index_raises_when_the_build_job_fails():
    """The submit call reports success even for a build that later fails, so the
    failure is only visible on the job record."""
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi(
        [
            _job(
                JobStatus.FAILED,
                error_message="could not detect dimension for 'embedding'",
            )
        ]
    )

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
        pytest.raises(RuntimeError, match="could not detect dimension"),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_embedding",
            columns=["embedding"],
            index_type="vector",
            metric="cosine",
            poll_interval_s=0,
        )


def test_create_index_raises_on_partial_success():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.PARTIALLY_SUCCEEDED, error_message="2 rows skipped")])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
        pytest.raises(RuntimeError, match="2 rows skipped"),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )


def test_create_index_failure_message_names_the_index_and_table():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.FAILED, error_message="boom")])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
        pytest.raises(RuntimeError) as excinfo,
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    message = str(excinfo.value)
    assert "ix_body" in message
    assert "db_1.public.docs" in message
    assert "boom" in message


def test_create_index_failure_without_a_message_still_raises():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.FAILED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
        pytest.raises(RuntimeError, match="failed"),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )


def test_create_bm25_index_succeeds_without_a_metric():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    built = _index_info(index_name="ix_body", index_type="bm25", columns=["body"], metric=None)
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED, result=SimpleNamespace(actual_instance=built))])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert indexes.requests[0].metric is None
    assert result.index_type == "bm25"
    assert result.metric is None
    assert result.status == "ready"


def test_create_index_wait_false_returns_pending_without_polling():
    client = _client()
    indexes = FakeIndexesApi(_submitted("job_9"))
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            wait=False,
        )

    assert jobs.calls == 0
    assert result.status == "pending"
    assert result.job_id == "job_9"
    assert result.index_name == "ix_body"
    assert result.columns == ["body"]


def test_create_index_accepts_an_inline_201_response():
    """A build the server finishes inline answers with the index itself, not a job."""
    client = _client()
    indexes = FakeIndexesApi(_index_info(index_name="ix_body", index_type="bm25", metric=None))
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["embedding"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert jobs.calls == 0
    assert result.status == "ready"
    assert result.job_id is None


def test_create_index_status_is_the_wire_string_not_the_enum_repr():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    built = _index_info(status=IndexStatus.PENDING)
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED, result=SimpleNamespace(actual_instance=built))])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_embedding",
            columns=["embedding"],
            index_type="vector",
            metric="cosine",
            poll_interval_s=0,
        )

    assert result.status == "pending"
    assert result.to_dict()["status"] == "pending"


def test_create_index_times_out_while_the_build_is_still_running():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.RUNNING)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
        pytest.raises(TimeoutError, match="running"),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            timeout_s=0.05,
            poll_interval_s=0,
        )


def test_create_index_wraps_api_exception_with_the_response_body():
    client = _client()
    indexes = FakeIndexesApi(ApiException(status=400, reason="Bad Request"))

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        pytest.raises(RuntimeError, match="Bad Request"),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
        )


def test_create_index_with_resolved_database_skips_read_probe():
    client = _client()
    databases = _ForbiddenDatabasesApi()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_databases_api", return_value=databases),
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert databases.read_calls == 0
    assert indexes.calls == [("conn_1", "public", "docs")]


def test_create_index_resolves_a_database_name():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "resolve_managed_database", return_value=_db()) as resolve,
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            "mydb",
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    resolve.assert_called_once_with("mydb")
    assert result.full_name == "db_1.public.docs"


def test_create_index_honors_a_non_default_schema():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            schema="analytics",
            index_name="ix_body",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert indexes.calls == [("conn_1", "analytics", "docs")]
    assert result.full_name == "db_1.analytics.docs"


def test_create_index_forwards_embedding_provider_options():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=["body"],
            index_type="vector",
            metric="cosine",
            dimensions=1536,
            embedding_provider_id="prov_1",
            output_column="body_vec",
            description="product descriptions",
            poll_interval_s=0,
        )

    request = indexes.requests[0]
    assert request.dimensions == 1536
    assert request.embedding_provider_id == "prov_1"
    assert request.output_column == "body_vec"
    assert request.description == "product descriptions"


@pytest.mark.parametrize("metric", ["l2", "cosine", "dot"])
def test_create_index_accepts_every_supported_metric(metric: str):
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="ix_embedding",
            columns=["embedding"],
            index_type="vector",
            metric=metric,
            poll_interval_s=0,
        )

    assert indexes.requests[0].metric == metric


def test_create_index_derives_the_cli_default_name():
    """Matches `hotdata indexes create` without --name, so both surfaces agree."""
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert indexes.requests[0].index_name == "docs_body_bm25"
    assert result.index_name == "docs_body_bm25"


def test_create_index_derived_name_joins_multiple_columns():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        client.create_index(
            _db(),
            "posts",
            columns=["title", "body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert indexes.requests[0].index_name == "posts_title_body_bm25"


def test_explicit_index_name_beats_the_derived_one():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        client.create_index(
            _db(),
            "docs",
            index_name="idx_custom",
            columns=["body"],
            index_type="bm25",
            poll_interval_s=0,
        )

    assert indexes.requests[0].index_name == "idx_custom"


def test_provider_backed_index_reports_the_source_column_to_query():
    """In provider-backed mode `columns` is the source text column and the index
    is built over a generated embedding column; queries name the source."""
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    built = _index_info(
        index_name="docs_body_vector",
        columns=["body_embedding"],
        source_column="body",
    )
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED, result=SimpleNamespace(actual_instance=built))])

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            columns=["body"],
            index_type="vector",
            metric="cosine",
            embedding_provider_id="sys_emb_openai",
            poll_interval_s=0,
        )

    assert indexes.requests[0].embedding_provider_id == "sys_emb_openai"
    assert result.source_column == "body"
    assert result.columns == ["body_embedding"]


def test_plain_vector_index_has_no_source_column():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi(
        [_job(JobStatus.SUCCEEDED, result=SimpleNamespace(actual_instance=_index_info()))]
    )

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            columns=["embedding"],
            index_type="vector",
            metric="cosine",
            poll_interval_s=0,
        )

    assert indexes.requests[0].embedding_provider_id is None
    assert result.source_column is None


def test_create_index_rejects_a_multi_column_vector_index():
    """The engine indexes only columns[0], so extra columns are silently dropped."""
    client = _client()
    with pytest.raises(ValueError, match="exactly one column"):
        client.create_index(
            _db(),
            "docs",
            columns=["embedding", "other"],
            index_type="vector",
            metric="cosine",
        )


@pytest.mark.parametrize(
    ("kwarg", "value"),
    [
        ("metric", "cosine"),
        ("dimensions", 1536),
        ("embedding_provider_id", "sys_emb_openai"),
        ("output_column", "body_vec"),
        ("description", "product descriptions"),
    ],
)
def test_create_index_rejects_vector_only_options_on_bm25(kwarg: str, value: object):
    client = _client()
    with pytest.raises(ValueError, match="vector indexes only"):
        client.create_index(
            _db(),
            "docs",
            columns=["body"],
            index_type="bm25",
            **{kwarg: value},  # type: ignore[arg-type]
        )


def test_create_index_requires_an_explicit_index_type():
    """Defaulting to the API's "sorted" would only surface at query time."""
    client = _client()
    with pytest.raises(TypeError, match="index_type"):
        client.create_index(  # type: ignore[call-arg]
            _db(),
            "docs",
            index_name="ix",
            columns=["body"],
        )


def test_create_index_rejects_an_unknown_index_type():
    client = _client()
    with pytest.raises(ValueError, match="index_type must be one of"):
        client.create_index(
            _db(),
            "docs",
            index_name="ix",
            columns=["body"],
            index_type="fulltext",  # type: ignore[arg-type]
        )


def test_create_index_rejects_an_unknown_metric():
    client = _client()
    with pytest.raises(ValueError, match="metric must be one of"):
        client.create_index(
            _db(),
            "docs",
            index_name="ix",
            columns=["embedding"],
            index_type="vector",
            metric="ip",  # type: ignore[arg-type]
        )


def test_create_index_rejects_a_metric_on_a_non_vector_index():
    client = _client()
    with pytest.raises(ValueError, match="vector indexes only"):
        client.create_index(
            _db(),
            "docs",
            index_name="ix",
            columns=["body"],
            index_type="bm25",
            metric="cosine",
        )


def test_create_index_rejects_dimensions_on_a_non_vector_index():
    client = _client()
    with pytest.raises(ValueError, match="dimensions applies to vector"):
        client.create_index(
            _db(),
            "docs",
            index_name="ix",
            columns=["body"],
            index_type="bm25",
            dimensions=1536,
        )


def test_create_index_rejects_empty_columns():
    client = _client()
    with pytest.raises(ValueError, match="at least one column"):
        client.create_index(
            _db(),
            "docs",
            index_name="ix",
            columns=[],
            index_type="bm25",
        )


def test_create_index_validates_before_calling_the_api():
    """A rejected argument must not reach the server or resolve the database."""
    client = _client()
    databases = _ForbiddenDatabasesApi()
    indexes = FakeIndexesApi(_submitted())

    with (
        patch.object(client, "_databases_api", return_value=databases),
        patch.object(client, "_indexes_api", return_value=indexes),
        pytest.raises(ValueError),
    ):
        client.create_index(
            "mydb",
            "docs",
            index_name="ix",
            columns=["body"],
            index_type="bm25",
            metric="cosine",
        )

    assert databases.read_calls == 0
    assert indexes.calls == []


def test_create_index_result_columns_are_not_aliased_to_the_argument():
    client = _client()
    indexes = FakeIndexesApi(_submitted())
    jobs = FakeJobsApi([_job(JobStatus.SUCCEEDED)])
    columns = ["body"]

    with (
        patch.object(client, "_indexes_api", return_value=indexes),
        patch.object(client, "_jobs_api", return_value=jobs),
    ):
        result = client.create_index(
            _db(),
            "docs",
            index_name="ix_body",
            columns=columns,
            index_type="bm25",
            poll_interval_s=0,
        )

    columns.append("title")
    assert result.columns == ["body"]
    assert indexes.requests[0].columns == ["body"]
