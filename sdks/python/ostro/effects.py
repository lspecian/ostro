"""Effect builders (HTTP, SQL, object store) and the semantic idempotency key (P-11).

Parity with the Rust SDK (``ostro_sdk::effects``, 06-09): each builder fills in the
unchanged WIT ``ostro:runtime@0.1.0`` request record (`HttpRequest`, `SqlRequest`,
`ObjectRequest` in `ostro._host`) and issues the existing ``http-fetch`` / ``sql-query`` /
``object-store`` call. The same inputs produce the same records as Rust.

```python
key = ctx.idempotency_key("charge")
resp = await (
    ctx.http.post("https://api.example.com/v1/charges")
    .secret_header("Authorization", "Bearer {{secret:stripe}}")
    .json(body)
    .idempotency_key(key)
    .timeout(10)
    .redact("Authorization")
    .send()
)
rows = await ctx.sql("orders").exactly_once().query("UPDATE t SET n = n + 1 WHERE id = $1", [7])
data = await ctx.object("files").get("a.txt")
```

Semantics (as in Rust):

- W1 templating: ``{{secret:NAME}}`` is expanded by the host in header values only, for
  origins the secret is granted to; bodies and URLs are never expanded.
- W2: a templated request never follows redirects.
- W3: ``redact`` names are replaced in the journal and logs; response headers with those
  names come back redacted too (live and replay identical).
- W4: any final HTTP status is a successful completion; inspect ``status``.
- W5: without an override the host sends ``ostro-<32 hex>`` derived from (run, op);
  `idempotency_key_for` gives a name-based key you can also embed in the body.
- W6: ``exactly_once`` runs one transactional statement with a token row.
- W7: object ``get`` -> bytes, ``put``/``head`` -> JSON metadata, ``delete`` -> nothing,
  ``presign-*`` -> URL; any other op fails ``unsupported_op``.
- W8: connector failures arrive as the op's failure and raise on ``await``.

Durations are seconds (int/float) or a `datetime.timedelta`, like everywhere in the SDK.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._host import (
    Header,
    HttpRequest,
    HttpResponse,
    ObjectRequest,
    RetryPolicy,
    SqlRequest,
    SqlResult,
    SqlValue,
)
from .errors import NonRetryable, from_failure
from .json import check_str, dumps, loads
from .ops import Op

if TYPE_CHECKING:
    from .loop import OstroLoop

__all__ = [
    "IDEMPOTENCY_KEY_DOMAIN",
    "HttpBuilder",
    "HttpClient",
    "HttpResponse",
    "ObjectClient",
    "ObjectHead",
    "ObjectPutInfo",
    "SqlBuilder",
    "SqlResult",
    "idempotency_key_for",
    "sql_value",
]

#: Domain separator of the semantic idempotency key (P-11).
IDEMPOTENCY_KEY_DOMAIN = b"ostro/idem/v1/name"

_SECRET_MARKER = "{{secret:"
_SQL_CASES = frozenset({"null", "boolean", "int8", "float8", "numeric", "text", "bytea", "json", "timestamptz"})
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def idempotency_key_for(run_id: str, name: str) -> str:
    """The semantic idempotency key for `name` in run `run_id` (P-11):
    ``"ostro-" + hex(sha256(b"ostro/idem/v1/name" || run_id || 0x00 || name))[:32]``.

    38 characters, deterministic per (run, name), identical to the Rust and TypeScript
    SDKs (pinned by ``conformance/vectors/idempotency.json``). Pure: no host call.
    """
    check_str(run_id, "run id")
    check_str(name, "idempotency key name")
    h = hashlib.sha256()
    h.update(IDEMPOTENCY_KEY_DOMAIN)
    h.update(run_id.encode("utf-8"))
    h.update(b"\x00")
    h.update(name.encode("utf-8"))
    return "ostro-" + h.hexdigest()[:32]


def _ms(d: Any, what: str) -> int | None:
    from .ctx import duration_ms

    return duration_ms(d, what)


def _protocol(what: str, case: str) -> NonRetryable:
    return NonRetryable("protocol", f"expected {what} output, got {case}")


class _EffectOp(Op):
    """An effect `Op` whose ``ok`` value is checked against the expected `op-output` case
    and mapped (Rust's per-builder decoders)."""

    __slots__ = ("_case", "_map")

    def __init__(self, loop: OstroLoop, op_id: int, kind: str, mapper: Any):
        super().__init__(loop, op_id, kind)
        self._case = kind
        self._map = mapper

    def __await__(self) -> Generator[Any, None, Any]:
        tag, value = yield from self._loop._op_future(self.id).__await__()
        if tag != "ok":
            raise from_failure(value)
        case, data = value
        if case != self._case:
            raise _protocol(self._case, case)
        return self._map(data)


def _identity(v: Any) -> Any:
    return v


def _retry_policy(policy: Any) -> RetryPolicy:
    if isinstance(policy, RetryPolicy):
        return policy
    to_policy = getattr(policy, "to_policy", None)
    if to_policy is None:
        raise TypeError("retry takes an ostro.Retry (or a WIT RetryPolicy)")
    return to_policy()


# ---------------------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------------------


class HttpBuilder:
    """Builder for one outbound HTTP request (WIT `http-request`). Methods return the
    builder, so calls chain; `send()` issues the request and returns an awaitable `Op`
    resolving to an `HttpResponse`."""

    __slots__ = ("_body", "_headers", "_key", "_loop", "_method", "_redact", "_retry", "_timeout", "_url")

    def __init__(self, method: str, url: str, loop: OstroLoop | None = None):
        self._loop = loop
        self._method = check_str(method, "HTTP method")
        self._url = check_str(url, "URL")
        self._headers: list[Header] = []
        self._body: bytes | None = None
        self._timeout: int | None = None
        self._key: str | None = None
        self._retry: RetryPolicy | None = None
        self._redact: list[str] = []

    def header(self, name: str, value: str) -> HttpBuilder:
        """Append a header (sent verbatim)."""
        self._headers.append(Header(name=check_str(name, "header name"), value=check_str(value, "header value")))
        return self

    def secret_header(self, name: str, template: str) -> HttpBuilder:
        """Append a header whose value carries a ``{{secret:NAME}}`` template, expanded by
        the host for granted origins only (W1). Raises `ValueError` if `template` has no
        ``{{secret:`` marker (a literal value here is almost certainly a leaked secret)."""
        if _SECRET_MARKER not in template:
            raise ValueError("secret_header value must contain a {{secret:NAME}} template")
        return self.header(name, template)

    def body(self, body: bytes | bytearray | memoryview | str) -> HttpBuilder:
        """Set the raw request body (a str is UTF-8 encoded; never templated, W1)."""
        if isinstance(body, str):
            body = check_str(body, "body").encode("utf-8")
        self._body = bytes(body)
        return self

    def json(self, value: Any) -> HttpBuilder:
        """JSON-encode `value` as the body and set ``content-type: application/json``
        (replacing any content-type header already set)."""
        data = dumps(value)
        self._headers = [h for h in self._headers if h.name.lower() != "content-type"]
        self._headers.append(Header(name="content-type", value="application/json"))
        self._body = data
        return self

    def idempotency_key(self, key: str) -> HttpBuilder:
        """Override the idempotency key the host sends (W5)."""
        self._key = check_str(key, "idempotency key")
        return self

    def timeout(self, d: Any) -> HttpBuilder:
        """Timeout of one attempt (seconds or a timedelta)."""
        self._timeout = _ms(d, "timeout")
        return self

    def retry(self, policy: Any) -> HttpBuilder:
        """Host retry policy (an `ostro.Retry`): connect failures, 408/425/429/5xx and
        transport loss are retried with the same idempotency key."""
        self._retry = _retry_policy(policy)
        return self

    def redact(self, *names: str | Iterable[str]) -> HttpBuilder:
        """Header names to redact in the journal and logs (W3). Takes names or iterables."""
        for n in names:
            items = [n] if isinstance(n, str) else list(n)
            self._redact.extend(check_str(x, "redact name") for x in items)
        return self

    def build(self) -> HttpRequest:
        """The WIT request record this builder describes."""
        return HttpRequest(
            method=self._method,
            url=self._url,
            headers=list(self._headers),
            body=self._body,
            timeout_ms=self._timeout,
            idempotency_key=self._key,
            retry=self._retry,
            redact=list(self._redact),
        )

    def send(self) -> Op:
        """Issue the request (``http-fetch``); await the op for an `HttpResponse`."""
        loop = _require_loop(self._loop)
        op = loop._h(True).http_fetch(self.build())
        return _EffectOp(loop, op, "http", _identity)


class HttpClient:
    """``ctx.http``: pick the method and URL."""

    __slots__ = ("_loop",)

    def __init__(self, loop: OstroLoop | None = None):
        self._loop = loop

    def get(self, url: str) -> HttpBuilder:
        return HttpBuilder("GET", url, self._loop)

    def post(self, url: str) -> HttpBuilder:
        return HttpBuilder("POST", url, self._loop)

    def put(self, url: str) -> HttpBuilder:
        return HttpBuilder("PUT", url, self._loop)

    def delete(self, url: str) -> HttpBuilder:
        return HttpBuilder("DELETE", url, self._loop)

    def request(self, method: str, url: str) -> HttpBuilder:
        return HttpBuilder(method, url, self._loop)


def _require_loop(loop: OstroLoop | None) -> OstroLoop:
    if loop is None:
        raise RuntimeError("this builder was created without a workflow context; use ctx.http / ctx.sql / ctx.object")
    return loop


# ---------------------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------------------


def sql_value(v: Any) -> SqlValue:
    """A WIT `sql-value` for a Python value: a ``(case, value)`` tuple with a known case is
    kept; ``None`` -> null, bool -> boolean, int -> int8, float -> float8, str -> text,
    bytes -> bytea, an aware/naive (UTC) datetime -> timestamptz (nanoseconds)."""
    if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str) and v[0] in _SQL_CASES:
        return v
    if v is None:
        return ("null", None)
    if isinstance(v, bool):
        return ("boolean", v)
    if isinstance(v, int):
        if not -(2**63) <= v < 2**63:
            raise ValueError("int SQL parameter does not fit in int8")
        return ("int8", v)
    if isinstance(v, float):
        return ("float8", v)
    if isinstance(v, str):
        return ("text", check_str(v, "SQL text parameter"))
    if isinstance(v, (bytes, bytearray, memoryview)):
        return ("bytea", bytes(v))
    if isinstance(v, _dt.datetime):
        at = v if v.tzinfo is not None else v.replace(tzinfo=_dt.timezone.utc)
        return ("timestamptz", (at - _EPOCH) // _dt.timedelta(microseconds=1) * 1000)
    raise TypeError(f"no SQL value mapping for {type(v).__name__}; pass a (case, value) tuple")


class SqlBuilder:
    """``ctx.sql(connection)``: statements against a named connection (WIT `sql-request`),
    at-least-once unless `exactly_once()` is called."""

    __slots__ = ("_connection", "_exactly_once", "_loop")

    def __init__(self, connection: str, loop: OstroLoop | None = None):
        self._loop = loop
        self._connection = check_str(connection, "connection name")
        self._exactly_once = False

    def exactly_once(self) -> SqlBuilder:
        """Exactly-once (W6): one statement in a transaction with a token row."""
        self._exactly_once = True
        return self

    def request(self, statement: str, params: Iterable[Any] = ()) -> SqlRequest:
        """The WIT request record for `statement` with positional `params`."""
        return SqlRequest(
            connection=self._connection,
            statement=check_str(statement, "SQL statement"),
            params=[sql_value(p) for p in params],
            exactly_once=self._exactly_once,
        )

    def query(self, statement: str, params: Iterable[Any] = ()) -> Op:
        """Run `statement` (``sql-query``); await the op for a `SqlResult`."""
        loop = _require_loop(self._loop)
        op = loop._h(True).sql_query(self.request(statement, params))
        return _EffectOp(loop, op, "sql", _identity)


# ---------------------------------------------------------------------------------------
# Object store
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ObjectPutInfo:
    """`put` result metadata."""

    e_tag: str | None = None


@dataclass(frozen=True)
class ObjectHead:
    """`head` result metadata."""

    size: int = 0
    last_modified: str = ""
    e_tag: str | None = None
    version: str | None = None


def _object_json(data: bytes) -> dict[str, Any]:
    value = loads(data)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise NonRetryable("invalid_json", f"expected a JSON object, got {type(value).__name__}")
    return value


def _put_info(data: bytes) -> ObjectPutInfo:
    v = _object_json(data)
    return ObjectPutInfo(e_tag=v.get("e_tag"))


def _head(data: bytes) -> ObjectHead:
    v = _object_json(data)
    return ObjectHead(
        size=v.get("size") or 0,
        last_modified=v.get("last_modified") or "",
        e_tag=v.get("e_tag"),
        version=v.get("version"),
    )


def _unit(data: bytes) -> None:
    return None


def _url(data: bytes) -> str:
    try:
        return bytes(data).decode("utf-8")
    except UnicodeDecodeError as e:
        raise NonRetryable("protocol", f"object URL not UTF-8: {e}") from None


def _bytes(data: bytes) -> bytes:
    return bytes(data)


class ObjectClient:
    """``ctx.object(connection)``: object-store operations (WIT `object-request`).
    Failures (including ``unsupported_op`` and missing objects) raise on ``await``."""

    __slots__ = ("_connection", "_loop")

    def __init__(self, connection: str, loop: OstroLoop | None = None):
        self._loop = loop
        self._connection = check_str(connection, "connection name")

    def request(self, op: str, key: str, body: bytes | None = None) -> ObjectRequest:
        """The WIT request record for `op` on `key`."""
        return ObjectRequest(
            connection=self._connection,
            op=check_str(op, "object op"),
            key=check_str(key, "object key"),
            body=None if body is None else bytes(body),
        )

    def _issue(self, req: ObjectRequest, mapper: Any) -> Op:
        loop = _require_loop(self._loop)
        op = loop._h(True).object_store(req)
        return _EffectOp(loop, op, "object", mapper)

    def raw(self, op: str, key: str, body: bytes | None = None) -> Op:
        """Any op string; resolves to the raw ``op-output.object`` bytes."""
        return self._issue(self.request(op, key, body), _bytes)

    def get(self, key: str) -> Op:
        """``get``: the object bytes."""
        return self._issue(self.request("get", key), _bytes)

    def put(self, key: str, body: bytes | bytearray | memoryview | str) -> Op:
        """``put``: store `body` (a str is UTF-8 encoded); resolves to `ObjectPutInfo`."""
        data = check_str(body, "object body").encode("utf-8") if isinstance(body, str) else bytes(body)
        return self._issue(self.request("put", key, data), _put_info)

    def head(self, key: str) -> Op:
        """``head``: resolves to `ObjectHead`."""
        return self._issue(self.request("head", key), _head)

    def delete(self, key: str) -> Op:
        """``delete``: resolves to ``None``."""
        return self._issue(self.request("delete", key), _unit)

    def presign_get(self, key: str) -> Op:
        """``presign-get``: resolves to a URL (expiry from the connection config)."""
        return self._issue(self.request("presign-get", key), _url)

    def presign_put(self, key: str) -> Op:
        """``presign-put``: resolves to a URL (expiry from the connection config)."""
        return self._issue(self.request("presign-put", key), _url)

