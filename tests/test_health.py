from __future__ import annotations

from unittest.mock import patch

import pytest
from hotdata.exceptions import ApiException

from hotdata_framework.client import HotdataClient
from hotdata_framework.health import workspace_health_lines


def test_workspace_health_ok():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    listing = type("L", (), {"connections": [object()]})()

    class FakeConnectionsApi:
        def list_connections(self):
            return listing

    with patch.object(client, "connections", return_value=FakeConnectionsApi()):
        ok, parts = workspace_health_lines(client)
    assert ok is True
    assert any("reachable" in p for p in parts)


def test_workspace_health_no_longer_reports_a_session(monkeypatch: pytest.MonkeyPatch):
    """Health output must not mention a session even when the environment offers
    one.

    HOTDATA_SANDBOX is set deliberately, and that is the whole point. An earlier
    version of this test built a client with no session at all — so on a revert
    that restored the property and the `if client.session_id:` line, the guard was
    simply false, no `sandbox` line was emitted, and the assertion passed with the
    regression fully present. Setting the variable is what makes the assertion
    reachable, because a revival of this feature would read it from here.
    """
    monkeypatch.setenv("HOTDATA_SANDBOX", "sb_should_reach_nothing")
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")
    listing = type("L", (), {"connections": [object()]})()

    class FakeConnectionsApi:
        def list_connections(self):
            return listing

    with patch.object(client, "connections", return_value=FakeConnectionsApi()):
        ok, parts = workspace_health_lines(client)
    assert ok is True
    assert "sb_should_reach_nothing" not in " ".join(parts)
    assert not any("sandbox" in p for p in parts)


def test_workspace_health_api_error():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class Boom:
        def list_connections(self):
            raise ApiException(status=500, reason="nope")

    with patch.object(client, "connections", return_value=Boom()):
        ok, parts = workspace_health_lines(client)
    assert ok is False
    assert parts == ["nope"]


def test_workspace_health_non_api_error():
    client = HotdataClient("k", "ws", host="https://api.hotdata.dev")

    class Boom:
        def list_connections(self):
            raise OSError("connection refused")

    with patch.object(client, "connections", return_value=Boom()):
        ok, parts = workspace_health_lines(client)
    assert ok is False
    assert parts == ["connection refused"]
