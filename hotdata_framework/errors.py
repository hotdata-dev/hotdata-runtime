from __future__ import annotations

import json
from collections.abc import Mapping

from hotdata.rest import ApiException

# The API explains a 409 with a machine-readable code, and the two it sends
# mean opposite things to a retry policy. RESOURCE_LOCKED is a refusal taken
# before any work: the insert that would have created the unit of work lost a
# unique-constraint race, so nothing was claimed and nothing was written.
# CONFLICT is the opposite — the request cannot succeed as posted, so retrying
# spends the whole budget arriving at the same answer.
_TERMINAL_CONFLICT_CODE = "CONFLICT"


class HotdataError(RuntimeError):
    """An API failure, carrying what a retry policy needs to decide.

    The message cannot be the discriminator: it is flattened and truncated for
    readability, so keying on it means substring-matching prose. ``status_code``
    and ``code`` are the machine-readable form of the same answer, and
    ``retry_after_seconds`` is the server's own estimate of how long the
    condition it just refused will last.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.retry_after_seconds = retry_after_seconds


class HotdataTransientError(HotdataError):
    pass


class HotdataTerminalError(HotdataError):
    pass


def _error_code(body: object) -> str | None:
    """The ``error.code`` an API error envelope carries, if this body is one.

    Not every 409 comes from an endpoint that speaks the envelope — a failed
    query result is reported as one and carries a result document instead — so
    a missing code is ordinary, and callers fall back to the status.
    """
    if not isinstance(body, (str, bytes, bytearray)):
        return None
    try:
        parsed: object = json.loads(body)
    except ValueError:
        return None
    if not isinstance(parsed, Mapping):
        return None
    error: object = parsed.get("error")
    if not isinstance(error, Mapping):
        return None
    code: object = error.get("code")
    return code if isinstance(code, str) else None


def _retry_after_seconds(headers: object) -> float | None:
    """``Retry-After`` as a number of seconds, when the response states one.

    Only the delta-seconds form is read. That is what the API sends, and the
    HTTP-date form would need a comparison against a server clock we do not
    have to be worth anything.
    """
    if not isinstance(headers, Mapping):
        return None
    raw: object = headers.get("Retry-After")
    if raw is None:
        # The SDK hands us urllib3's case-insensitive mapping and the API sends
        # the header lower-cased, so the direct hit is what normally answers.
        # Fall back for any plain dict that reaches us instead — a missed
        # header is silent, and silence here reads as "the server asked for
        # nothing".
        raw = next((v for k, v in headers.items() if str(k).lower() == "retry-after"), None)
    if raw is None:
        return None
    try:
        seconds = float(str(raw).strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def _error_class(status_code: int, code: str | None) -> type[HotdataError]:
    if status_code == 409 and code == _TERMINAL_CONFLICT_CODE:
        # The request cannot succeed as posted — an upload already consumed
        # with nothing to replay, a receipt naming a different target, an
        # incompatible column type. Every retry reaches the same 409.
        return HotdataTerminalError
    if status_code in (408, 409, 425, 429):
        return HotdataTransientError
    if status_code == 501:
        # Not Implemented is a permanent capability gap (e.g. the storage
        # backend cannot issue presigned URLs) — retrying cannot succeed.
        return HotdataTerminalError
    if 500 <= status_code <= 599:
        return HotdataTransientError
    return HotdataTerminalError


def classify_sdk_error(error: Exception) -> HotdataError:
    if isinstance(error, HotdataError):
        # Already classified. A caller that read transience off a typed status
        # -- an interrupted query run, say -- knows more than this function can
        # recover from the exception, and the fallback below would demote it to
        # terminal and cost the retry.
        return error
    if isinstance(error, TimeoutError):
        return HotdataTransientError(str(error))
    if isinstance(error, ConnectionError):
        return HotdataTransientError(str(error))
    if isinstance(error, ApiException):
        status_code = int(error.status or 0)
        message = f"{status_code}: {error.reason or 'unknown error'}"
        # The response body is where the API explains itself (e.g. which
        # header is missing) — without it "400: Bad Request" is undebuggable.
        body: object = getattr(error, "body", None)
        if body:
            message = f"{message} — {' '.join(str(body).split())[:500]}"
        code = _error_code(body)
        return _error_class(status_code, code)(
            message,
            status_code=status_code,
            code=code,
            retry_after_seconds=_retry_after_seconds(getattr(error, "headers", None)),
        )
    return HotdataTerminalError(str(error))
