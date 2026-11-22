"""The one error type of the client."""

from __future__ import annotations

from typing import Any, Dict, Optional


class OstroError(Exception):
    """A failed call.

    ``status`` is the HTTP status (``None`` when no response arrived), ``code`` the error
    code of the REST error body ``{"error": {"code", "message", "details"}}``. Transport
    failures use ``code="unreachable"`` (no connection) or ``code="timeout"``; a non-JSON
    error response uses ``code="http_<status>"``.
    """

    def __init__(
        self,
        status: Optional[int],
        code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(f"{code}: {message}" if status is None else f"HTTP {status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.details: Dict[str, Any] = details if details is not None else {}

    @property
    def reason(self) -> Optional[str]:
        """``details.reason`` when the server sent one (e.g. ``time_skipping_disabled``)."""
        r = self.details.get("reason") if isinstance(self.details, dict) else None
        return r if isinstance(r, str) else None

    def __repr__(self) -> str:
        return (
            f"OstroError(status={self.status!r}, code={self.code!r}, "
            f"message={self.message!r}, details={self.details!r})"
        )
