"""The Ostro REST client (standard library only: ``urllib`` + ``json``).

Mirrors the Rust reference client (``crates/ostro-client/src/typed.rs``). Responses are the
server's JSON objects as plain ``dict``s; every failure raises :class:`OstroError`.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from .errors import OstroError

DEFAULT_SERVER = "http://127.0.0.1:7700"

#: The server caps one result long-poll at 60 s; longer waits are a loop of polls.
MAX_POLL_S = 60.0

#: Extra time a replay request may take on top of the client timeout (the server replays
#: up to 1000 journals per request).
REPLAY_EXTRA_TIMEOUT_S = 300.0

Json = Any
Duration = Union[str, int, float]

_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")


def parse_duration(d: Duration) -> float:
    """``"30s"`` / ``"500ms"`` / ``"2m"`` / ``"72h"`` / ``"1d"`` or a number of seconds -> seconds."""
    if isinstance(d, bool):
        raise ValueError(f"not a duration: {d!r}")
    if isinstance(d, (int, float)):
        if d < 0:
            raise ValueError(f"negative duration: {d!r}")
        return float(d)
    m = _DURATION.match(d)
    if not m:
        raise ValueError(f"not a duration (use e.g. '30s', '500ms', '72h'): {d!r}")
    return float(m.group(1)) * _UNITS[m.group(2)]


def _ms(seconds: float) -> str:
    return f"{int(round(seconds * 1000))}ms"


def sha256_hex(data: bytes) -> str:
    """The lowercase hex SHA-256 of ``data`` (a module's id)."""
    return hashlib.sha256(data).hexdigest()


class Client:
    """A client for one Ostro server.

    ``server`` defaults to ``$OSTRO_SERVER``, else ``http://127.0.0.1:7700`` (``ostro dev``).
    ``timeout`` (seconds) bounds every request; a result long-poll gets its wait on top.
    """

    def __init__(self, server: Optional[str] = None, timeout: float = 30.0) -> None:
        server = server or os.environ.get("OSTRO_SERVER") or DEFAULT_SERVER
        if "://" not in server:
            server = "http://" + server
        self.server = server.rstrip("/")
        self.timeout = float(timeout)

    def __repr__(self) -> str:
        return f"Client(server={self.server!r}, timeout={self.timeout!r})"

    # --- transport ------------------------------------------------------------------

    def _url(self, segments: Sequence[str], params: Optional[Iterable[Tuple[str, Any]]]) -> str:
        # One percent-encoded segment per value: a tenant-qualified name (`acme/w`) stays
        # one segment.
        path = "/".join(urllib.parse.quote(str(s), safe="") for s in segments)
        url = f"{self.server}/{path}"
        if params:
            q = [(k, _qv(v)) for k, v in params if v is not None]
            if q:
                url += "?" + urllib.parse.urlencode(q)
        return url

    def _request(
        self,
        method: str,
        template: str,
        segments: Sequence[str],
        params: Optional[Iterable[Tuple[str, Any]]] = None,
        body: Json = None,
        *,
        raw: Optional[bytes] = None,
        content_type: str = "application/json",
        extra_timeout: float = 0.0,
    ) -> Json:
        """One request. ``template`` is the route (``routes.ROUTES``) the call implements."""
        del template  # documentation only (checked by the unit tests)
        url = self._url(segments, params)
        data: Optional[bytes] = None
        headers = {"Accept": "application/json"}
        if raw is not None:
            data = raw
            headers["Content-Type"] = content_type
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout + extra_timeout) as resp:
                status = resp.status
                text = resp.read()
        except urllib.error.HTTPError as e:
            raise _api_error(e.code, e.read()) from None
        except urllib.error.URLError as e:
            if isinstance(e.reason, (socket.timeout, TimeoutError)):
                raise OstroError(None, "timeout", f"request to the Ostro server at {self.server} timed out") from None
            raise OstroError(None, "unreachable", f"cannot reach the Ostro server at {self.server}: {e.reason}") from None
        except (socket.timeout, TimeoutError):
            raise OstroError(None, "timeout", f"request to the Ostro server at {self.server} timed out") from None
        except (ConnectionError, OSError) as e:
            raise OstroError(None, "unreachable", f"cannot reach the Ostro server at {self.server}: {e}") from None
        if not text.strip():
            return None
        try:
            return json.loads(text)
        except ValueError:
            raise OstroError(
                status,
                f"http_{status}",
                f"the server at {self.server} answered HTTP {status} with a non-JSON body",
            ) from None

    # --- modules and workflows ------------------------------------------------------

    def upload_module(self, wasm: bytes, name: Optional[str] = None, tenant: Optional[str] = None) -> Dict[str, Any]:
        """``PUT /v1/modules/{sha256}``: upload a Wasm component (``created`` false when it
        already existed). Returns ``{"created", "module": {"sha256", "workflows", ...}}``."""
        sha = sha256_hex(wasm)
        return self._request(
            "PUT",
            "/v1/modules/{sha256}",
            ["v1", "modules", sha],
            [("name", name), ("tenant", tenant)],
            raw=bytes(wasm),
            content_type="application/wasm",
        )

    def workflows(self) -> List[Dict[str, Any]]:
        """``GET /v1/workflows``: every workflow with its versions."""
        return self._request("GET", "/v1/workflows", ["v1", "workflows"])["workflows"]

    def activate(self, workflow: str, version: int) -> Dict[str, Any]:
        """``PUT /v1/workflows/{name}/active``: make ``version`` the active one."""
        return self._request(
            "PUT", "/v1/workflows/{name}/active", ["v1", "workflows", workflow, "active"], body={"version": version}
        )

    # --- runs -----------------------------------------------------------------------

    def start_run(
        self,
        workflow: str,
        input: Json = None,
        *,
        idempotency_key: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
        conflict_policy: Optional[str] = None,
        version: Optional[int] = None,
        queue: Optional[str] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        """``POST /v1/workflows/{name}/runs``: start a run. Returns ``{"run_id", "created"}``.

        Further body fields of the API (``retention``, ``priority``, ``delay_ms``, ...) pass
        through ``extra``.
        """
        body: Dict[str, Any] = {"input": input}
        for k, v in (
            ("idempotency_key", idempotency_key),
            ("conflict_policy", conflict_policy),
            ("version", version),
            ("queue", queue),
        ):
            if v is not None:
                body[k] = v
        if tags:
            body["tags"] = list(tags)
        body.update({k: v for k, v in extra.items() if v is not None})
        return self._request("POST", "/v1/workflows/{name}/runs", ["v1", "workflows", workflow, "runs"], body=body)

    def get_run(self, run_id: str) -> Dict[str, Any]:
        """``GET /v1/runs/{run_id}``: the run's record (``status``, ``result``, ...)."""
        return self._request("GET", "/v1/runs/{run_id}", ["v1", "runs", run_id])

    def list_runs(
        self,
        *,
        workflow: Optional[str] = None,
        status: Optional[str] = None,
        queue: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """``GET /v1/runs``: one page ``{"runs", "next_page_token"}``."""
        params: List[Tuple[str, Any]] = [("workflow", workflow), ("status", status), ("queue", queue)]
        params += [("tag", t) for t in (tags or [])]
        params += [("limit", limit), ("page_token", page_token)]
        return self._request("GET", "/v1/runs", ["v1", "runs"], params)

    def history(self, run_id: str, *, from_seq: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        """``GET /v1/runs/{run_id}/history``: one page ``{"events", "next_from_seq"}``."""
        return self._request(
            "GET",
            "/v1/runs/{run_id}/history",
            ["v1", "runs", run_id, "history"],
            [("from_seq", from_seq), ("limit", limit)],
        )

    def result(self, run_id: str, wait: Optional[Duration] = None) -> Dict[str, Any]:
        """``GET /v1/runs/{run_id}/result``: the outcome, long-polling up to ``wait``
        (``"30s"`` or seconds; default no wait). ``completed`` is false when the wait ended
        first."""
        total = parse_duration(wait) if wait is not None else 0.0
        deadline = time.monotonic() + total
        left = total
        while True:
            poll = min(left, MAX_POLL_S)
            r = self._request(
                "GET",
                "/v1/runs/{run_id}/result",
                ["v1", "runs", run_id, "result"],
                [("wait", _ms(poll))],
                extra_timeout=poll,
            )
            if r.get("completed") or left <= 0.0:
                return r
            left = max(0.0, deadline - time.monotonic())

    # --- lifecycle ------------------------------------------------------------------

    def signal(self, run_id: str, name: str, payload: Json = None, *, request_id: Optional[str] = None) -> Dict[str, Any]:
        """``POST /v1/runs/{run_id}/signals/{name}``: ``{"accepted", "duplicate"}``."""
        body: Dict[str, Any] = {"payload": payload}
        if request_id is not None:
            body["request_id"] = request_id
        return self._request(
            "POST", "/v1/runs/{run_id}/signals/{name}", ["v1", "runs", run_id, "signals", name], body=body
        )

    def cancel(self, run_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        """``POST /v1/runs/{run_id}/cancel``: cooperative cancellation."""
        return self._request("POST", "/v1/runs/{run_id}/cancel", ["v1", "runs", run_id, "cancel"], body={"reason": reason})

    def terminate(self, run_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        """``POST /v1/runs/{run_id}/terminate``: immediate termination."""
        return self._request(
            "POST", "/v1/runs/{run_id}/terminate", ["v1", "runs", run_id, "terminate"], body={"reason": reason}
        )

    def retry(self, run_id: str) -> Dict[str, Any]:
        """``POST /v1/runs/{run_id}/retry``: retry a failed run (``{"mode", "run_id"}``)."""
        return self._request("POST", "/v1/runs/{run_id}/retry", ["v1", "runs", run_id, "retry"], body={})

    def complete_token(self, token: str, payload: Json = None) -> Dict[str, Any]:
        """``POST /v1/tokens/complete``: complete an external-completion token."""
        return self._request(
            "POST", "/v1/tokens/complete", ["v1", "tokens", "complete"], body={"token": token, "payload": payload}
        )

    # --- dev clock and replay -------------------------------------------------------

    def dev_time(self) -> Dict[str, Any]:
        """``GET /v1/dev/time``: ``{"now_ms", "offset_ms", "mode", "idle", "next_due_ms"}``
        (409 ``time_skipping_disabled`` unless ``ostro dev --ephemeral --time-skipping``)."""
        return self._request("GET", "/v1/dev/time", ["v1", "dev", "time"])

    def dev_skip(self, by: Optional[Duration] = None, *, to_ms: Optional[int] = None) -> Dict[str, Any]:
        """``POST /v1/dev/time/skip``: move the dev clock forward ``by`` (``"24h"`` or
        seconds) or to ``to_ms`` (exactly one). Returns ``{"now_ms", "offset_ms", "skipped_ms"}``."""
        if (by is None) == (to_ms is None):
            raise ValueError("dev_skip needs exactly one of by / to_ms")
        if by is not None:
            body: Dict[str, Any] = {"by": by if isinstance(by, str) else _ms(parse_duration(by))}
        else:
            body = {"to_ms": to_ms}
        return self._request("POST", "/v1/dev/time/skip", ["v1", "dev", "time", "skip"], body=body)

    def replay(
        self,
        sha: str,
        *,
        runs: Optional[Sequence[str]] = None,
        sample: Optional[int] = None,
        workflow: Optional[str] = None,
        status: Optional[str] = None,
        allow_missing: bool = False,
    ) -> Dict[str, Any]:
        """``POST /v1/modules/{sha}/replay``: replay recorded runs against module ``sha``.
        Pass ``runs`` (ids) or ``sample`` (n, optionally filtered by workflow/status).
        Returns ``{"module_sha256", "verdicts", "summary"}``."""
        if (runs is None) == (sample is None):
            raise ValueError("replay needs exactly one of runs / sample")
        body: Dict[str, Any] = {"runs": list(runs)} if runs is not None else {"sample": sample}
        if sample is not None:
            if workflow is not None:
                body["workflow"] = workflow
            if status is not None:
                body["status"] = status
        if allow_missing:
            body["allow_missing"] = True
        return self._request(
            "POST",
            "/v1/modules/{sha}/replay",
            ["v1", "modules", sha, "replay"],
            body=body,
            extra_timeout=REPLAY_EXTRA_TIMEOUT_S,
        )


def _qv(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _api_error(status: int, text: bytes) -> OstroError:
    try:
        body = json.loads(text) if text.strip() else None
    except ValueError:
        body = None
    err: Mapping[str, Any] = body.get("error") if isinstance(body, dict) else None  # type: ignore[assignment]
    if isinstance(err, dict):
        details = err.get("details")
        return OstroError(
            status,
            str(err.get("code") or f"http_{status}"),
            str(err.get("message") or ""),
            details if isinstance(details, dict) else {},
        )
    msg = text.decode("utf-8", "replace").strip()
    return OstroError(status, f"http_{status}", msg)
