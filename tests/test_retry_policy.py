"""A POST must never be replayed *by the transport* because of a response status.

Re-sending a load the server is still working on collides with the write lock
the first attempt holds, and the duplicate is refused — so a blind transport
replay buys nothing and spends an attempt. This is a claim about the transport,
which sees a method and a status and cannot know what it would be re-sending.
It is not a claim that loads must never be retried: ``ManagedDatabaseClient``
retries them at the call layer, where the same ``upload_id`` goes back out and
the API replays its receipt for that id instead of applying the load twice.

The generated SDK already draws this line — ``hotdata._retry`` retries
a *pre-response* connection reset on any method (the stale pooled socket case,
where the server did no work) while leaving read timeouts and status retries
idempotent-only. This wrapper used to pass its own ``retries=`` into
``Configuration``, which replaced that policy wholesale with one that listed
POST alongside a 502/503/504 forcelist — so a gateway timing out a long request
produced a silent duplicate of it.

These tests pin the resulting policy rather than the absence of an argument,
so re-introducing an override that is unsafe for POST fails here.
"""

from __future__ import annotations

from hotdata_framework.client import HotdataClient

WS = "work_test0000000000000000000000"


def _retry_policy():
    client = HotdataClient(api_key="hd_test", workspace_id=WS, host="https://example.invalid")
    return client._config.retries


def test_no_status_forcelist() -> None:
    """A status code means the request reached the server, so retrying one is a
    decision about idempotency that the transport layer cannot make."""
    retry = _retry_policy()
    assert not retry.status_forcelist, retry.status_forcelist
    assert retry.status == 0


def test_post_is_not_read_or_status_retried() -> None:
    """``allowed_methods`` gates both read and status retries. POST outside it is
    what stops a request the server may already be processing from being sent
    twice."""
    retry = _retry_policy()
    assert "POST" not in retry.allowed_methods
    assert "GET" in retry.allowed_methods


def test_pre_response_connection_reset_still_retries_any_method() -> None:
    """The case the wrapper's own override was reaching for, kept: a reset before
    any response means the request never landed, so a POST retry is safe."""
    from urllib3.exceptions import ProtocolError

    # `_is_connection_error` is urllib3-private, and urllib3 reaches this package
    # only as an unpinned transitive dependency — so an upstream rename breaks
    # this test for reasons unrelated to this repo. Accepted knowingly: it is the
    # only hook that separates a reset from any other ProtocolError without
    # standing up a live socket.
    retry = _retry_policy()
    cause = ConnectionResetError(54, "Connection reset by peer")
    reset = ProtocolError("Connection aborted.", cause)
    assert retry._is_connection_error(reset)
    # A ProtocolError with no connection-level cause is NOT reclassified: only a
    # reset proves the request never landed. Read timeouts never reach this branch
    # at all — urllib3 raises those as ReadTimeoutError, and they stay method-gated.
    assert not retry._is_connection_error(ProtocolError("Connection aborted.", None))
