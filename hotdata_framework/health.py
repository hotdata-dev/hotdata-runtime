from __future__ import annotations

from hotdata.exceptions import ApiException

from hotdata_framework.client import HotdataClient


def workspace_health_lines(client: HotdataClient) -> tuple[bool, list[str]]:
    """Return ``(ok, parts)`` where ``parts`` are short markdown fragments.

    On failure, ``ok`` is False and ``parts`` is a single-element list with the error text.
    """
    try:
        listing = client.connections().list_connections()
        n = len(listing.connections)
        lines = [
            "**API** reachable",
            f"**workspace** `{client.workspace_id}`",
            f"**connections** {n}",
        ]
        return True, lines
    except ApiException as e:
        return False, [e.reason or str(e)]
    except Exception as e:
        return False, [str(e)]
