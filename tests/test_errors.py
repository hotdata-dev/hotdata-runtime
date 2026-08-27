"""Error message construction: the API's response body must survive.

"400: Bad Request" alone is undebuggable; the body carries the server's
actual explanation (e.g. which header was missing).
"""

from __future__ import annotations

from hotdata.rest import ApiException

from hotdata_framework.databases import api_error_message
from hotdata_framework.errors import (
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)

BODY = '{"error":{"code":"BAD_REQUEST","message":"X-Database-Id header is required"}}'


def test_classify_sdk_error_includes_response_body() -> None:
    err = classify_sdk_error(ApiException(status=400, reason="Bad Request", body=BODY))
    assert isinstance(err, HotdataTerminalError)
    assert "400: Bad Request" in str(err)
    assert "X-Database-Id header is required" in str(err)


def test_classify_sdk_error_without_body_keeps_short_form() -> None:
    err = classify_sdk_error(ApiException(status=409, reason="Conflict"))
    assert isinstance(err, HotdataTransientError)
    assert str(err) == "409: Conflict"


LOCKED = (
    '{"error":{"code":"RESOURCE_LOCKED","message":"another operation is already '
    'running for conn:c1:public:_dlt_pipeline_state; retry shortly"}}'
)
CONFLICT = '{"error":{"code":"CONFLICT","message":"upload already consumed"}}'


def test_resource_locked_is_transient_and_names_itself() -> None:
    """A lock refusal is taken before any work — the insert that would have
    created the unit of work lost a unique-constraint race — so nothing was
    claimed and a retry is safe."""
    err = classify_sdk_error(ApiException(status=409, reason="Conflict", body=LOCKED))
    assert isinstance(err, HotdataTransientError)
    assert err.status_code == 409
    assert err.code == "RESOURCE_LOCKED"


def test_conflict_is_terminal_despite_being_a_409() -> None:
    """A CONFLICT cannot succeed as posted, so retrying it spends the entire
    budget to arrive at the same 409. Classifying every 409 as transient meant
    permanent conflicts burned the full ramp before surfacing."""
    err = classify_sdk_error(ApiException(status=409, reason="Conflict", body=CONFLICT))
    assert isinstance(err, HotdataTerminalError)
    assert err.code == "CONFLICT"


def test_a_409_that_is_not_an_error_envelope_stays_transient() -> None:
    """Not every 409 comes from an endpoint that speaks the envelope: a failed
    query result is reported as one and carries a result document. With no code
    to read, the status decides, and the classification is unchanged."""
    body = '{"result_id":"rslt1","status":"failed","error_message":"query panicked"}'
    err = classify_sdk_error(ApiException(status=409, reason="Conflict", body=body))
    assert isinstance(err, HotdataTransientError)
    assert err.code is None


def _locked(headers: object) -> ApiException:
    """A lock refusal carrying response headers.

    ``ApiException`` only populates ``headers`` from a real ``http_resp``, so a
    hand-built one sets it after construction — the same attribute the SDK
    assigns."""
    err = ApiException(status=409, reason="Conflict", body=LOCKED)
    err.headers = headers
    return err


def test_retry_after_is_read_from_the_response() -> None:
    assert classify_sdk_error(_locked({"Retry-After": "5"})).retry_after_seconds == 5.0


def test_retry_after_is_found_however_the_header_is_cased() -> None:
    """The API sends it lower-cased. urllib3's mapping is case-insensitive so
    the direct lookup normally answers, but a plain dict must not silently read
    as "the server asked for nothing"."""
    assert classify_sdk_error(_locked({"retry-after": "5"})).retry_after_seconds == 5.0


def test_an_unparseable_retry_after_is_ignored_rather_than_fatal() -> None:
    """Only the delta-seconds form is read. An HTTP-date would need a server
    clock to be worth anything, and a malformed header must not become an
    exception raised while classifying another exception."""
    stamp = "Wed, 21 Oct 2026 07:28:00 GMT"
    assert classify_sdk_error(_locked({"Retry-After": stamp})).retry_after_seconds is None


def test_headers_that_are_not_a_mapping_are_ignored() -> None:
    assert classify_sdk_error(_locked(object())).retry_after_seconds is None


def test_a_body_that_is_not_json_does_not_break_classification() -> None:
    """A proxy or load balancer can answer with HTML the API never wrote."""
    err = classify_sdk_error(ApiException(status=409, reason="Conflict", body="<html>nope</html>"))
    assert isinstance(err, HotdataTransientError)
    assert err.code is None


def test_classify_sdk_error_truncates_and_flattens_body() -> None:
    noisy = "x\n" * 1000
    err = classify_sdk_error(ApiException(status=500, reason="ISE", body=noisy))
    assert "\n" not in str(err)
    assert len(str(err)) < 600


def test_api_error_message_includes_body() -> None:
    msg = api_error_message(ApiException(status=400, reason="Bad Request", body=BODY))
    assert msg.startswith("Bad Request: ")
    assert "X-Database-Id header is required" in msg


def test_api_error_message_without_body() -> None:
    assert api_error_message(ApiException(status=404, reason="Not Found")) == "Not Found"
