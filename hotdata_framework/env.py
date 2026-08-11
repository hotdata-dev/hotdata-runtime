from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from hotdata import ApiClient, Configuration
from hotdata.api.workspaces_api import WorkspacesApi


def normalize_host(url: str) -> str:
    u = url.rstrip("/")
    if u.endswith("/v1"):
        u = u[:-3]
    parsed = urlparse(u)
    if not parsed.scheme or not parsed.netloc:
        return u
    return f"{parsed.scheme}://{parsed.netloc}"


def default_api_key() -> str:
    return os.environ.get("HOTDATA_API_KEY", "")


def explicit_workspace_id() -> str | None:
    return os.environ.get("HOTDATA_WORKSPACE")


def default_host() -> str:
    raw = os.environ.get("HOTDATA_API_URL", "https://api.hotdata.dev")
    return normalize_host(raw)


def list_workspaces(api_key: str, host: str):
    cfg = Configuration(
        host=host,
        api_key=api_key,
        workspace_id=None,
    )
    with ApiClient(cfg) as api:
        listing = WorkspacesApi(api).list_workspaces()
    return listing.workspaces


@dataclass(frozen=True)
class WorkspaceSelection:
    workspace_id: str
    source: str
    workspaces: list


def resolve_workspace_selection(api_key: str, host: str) -> WorkspaceSelection:
    explicit = explicit_workspace_id()
    if explicit:
        return WorkspaceSelection(
            workspace_id=explicit,
            source="explicit_env",
            workspaces=[],
        )
    workspaces = list_workspaces(api_key, host)
    if not workspaces:
        raise RuntimeError("No Hotdata workspaces found for this API key.")
    active = [w for w in workspaces if w.active]
    chosen = active[0] if active else workspaces[0]
    return WorkspaceSelection(
        workspace_id=chosen.public_id,
        source="active" if active else "first",
        workspaces=workspaces,
    )


def pick_workspace(api_key: str, host: str) -> str:
    selection = resolve_workspace_selection(api_key, host)
    return selection.workspace_id
