"""Effect builders (09-12): the WIT request records equal the Rust builders' (06-09) for the
same inputs, and responses decode like Rust's per-builder decoders."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from conftest import FIRST_OP, FakeHost, run_in_loop
from ostro import NonRetryable, Retry
from ostro._host import (
    ErrorInfo,
    Header,
    HttpRequest,
    HttpResponse,
    ObjectRequest,
    RetryPolicy,
    SqlRequest,
    SqlResult,
)
from ostro.effects import HttpBuilder, ObjectClient, ObjectHead, ObjectPutInfo, SqlBuilder, sql_value
from ostro.errors import InvalidString, ProtocolError


def _ok(op: int, case: str, value: Any) -> tuple[int, Any]:
    return (op, ("ok", (case, value)))


def _recorded(host: FakeHost, method: str) -> list[Any]:
    return [args[0] for m, args in host.calls if m == method]


# --- HTTP -------------------------------------------------------------------------


def test_http_record_matches_the_rust_builder() -> None:
    # Rust: ctx.http().post(URL).header("x-trace", "t1")
    #   .secret_header("Authorization", "Bearer {{secret:stripe}}")
    #   .header("Content-Type", "text/plain").json(&json!({"amount":100,"currency":"usd"}))?
    #   .idempotency_key("k-1").timeout(Duration::from_millis(2500))
    #   .retry(RetryPolicy{max_attempts:5, initial 200 ms, max 5 s, 1.5, ["bad"]})
    #   .redact(["Authorization"]).build()
    req = (
        HttpBuilder("POST", "https://api.example.com/v1/charges")
        .header("x-trace", "t1")
        .secret_header("Authorization", "Bearer {{secret:stripe}}")
        .header("Content-Type", "text/plain")
        .json({"amount": 100, "currency": "usd"})
        .idempotency_key("k-1")
        .timeout(2.5)
        .retry(Retry(max_attempts=5, initial=0.2, max=5, backoff=1.5, non_retryable=["bad"]))
        .redact("Authorization")
        .build()
    )
    assert req == HttpRequest(
        method="POST",
        url="https://api.example.com/v1/charges",
        headers=[
            Header("x-trace", "t1"),
            Header("Authorization", "Bearer {{secret:stripe}}"),
            Header("content-type", "application/json"),
        ],
        body=b'{"amount":100,"currency":"usd"}',
        timeout_ms=2500,
        idempotency_key="k-1",
        retry=RetryPolicy(
            max_attempts=5,
            initial_interval_ms=200,
            max_interval_ms=5000,
            backoff_coefficient=1.5,
            non_retryable_kinds=["bad"],
        ),
        redact=["Authorization"],
    )


def test_http_defaults_are_empty_like_rust_new() -> None:
    assert HttpBuilder("GET", "https://x/").build() == HttpRequest(
        method="GET", url="https://x/", headers=[], body=None, timeout_ms=None, idempotency_key=None, retry=None, redact=[]
    )


def test_http_retry_defaults_fill_like_rust_partial_policy() -> None:
    req = HttpBuilder("GET", "https://x/").retry(Retry()).build()
    assert req.retry == RetryPolicy(3, 1000, 60000, 2.0, [])


def test_http_body_and_timedelta_timeout_and_redact_iterables() -> None:
    req = (
        HttpBuilder("PUT", "https://x/")
        .body("héllo")
        .timeout(dt.timedelta(seconds=3))
        .redact(["a", "b"], "c")
        .build()
    )
    assert req.body == "héllo".encode()
    assert req.timeout_ms == 3000
    assert req.redact == ["a", "b", "c"]


def test_secret_header_requires_a_template() -> None:
    with pytest.raises(ValueError, match="secret"):
        HttpBuilder("GET", "https://x/").secret_header("Authorization", "Bearer sk_live_123")


def test_lone_surrogates_rejected_before_the_host() -> None:
    with pytest.raises(InvalidString):
        HttpBuilder("GET", "https://x/").header("x", "\ud800")


def test_ctx_http_sends_the_record_and_decodes_the_response(fake_host: FakeHost) -> None:
    resp = HttpResponse(status=201, headers=[Header("x", "y")], body=b"{}")
    fake_host.script_wait([_ok(FIRST_OP, "http", resp)])

    async def body(ctx: Any) -> Any:
        op = ctx.http.get("https://x/a").header("accept", "application/json").send()
        return await op

    assert run_in_loop(fake_host, body) == resp
    assert _recorded(fake_host, "http_fetch") == [
        HttpRequest(method="GET", url="https://x/a", headers=[Header("accept", "application/json")])
    ]


@pytest.mark.parametrize("method", ["get", "post", "put", "delete"])
def test_http_client_methods(fake_host: FakeHost, method: str) -> None:
    fake_host.script_wait([_ok(FIRST_OP, "http", HttpResponse(200, [], b""))])

    async def body(ctx: Any) -> Any:
        return await getattr(ctx.http, method)("https://x/").send()

    run_in_loop(fake_host, body)
    assert _recorded(fake_host, "http_fetch")[0].method == method.upper()


def test_http_failure_and_wrong_output_case(fake_host: FakeHost) -> None:
    fake_host.script_wait(
        [(FIRST_OP, ("err", ("non-retryable", ErrorInfo("egress_denied", "nope"))))],
        [_ok(FIRST_OP + 1, "timer", None)],
    )

    async def body(ctx: Any) -> Any:
        out = []
        for _ in range(2):
            try:
                await ctx.http.get("https://x/").send()
            except NonRetryable as e:
                out.append(e.kind)
        return out

    assert run_in_loop(fake_host, body) == ["egress_denied", "protocol"]


def test_http_send_inside_a_step_is_a_protocol_error(fake_host: FakeHost) -> None:
    async def body(ctx: Any) -> Any:
        async def s() -> None:
            ctx.http.get("https://x/").send()

        from ostro._host import StepOptions
        from ostro.step import run_step

        try:
            await run_step("s", StepOptions(), s)
        except ProtocolError as e:
            return e.kind

    assert run_in_loop(fake_host, body) == "sdk_protocol"
    assert _recorded(fake_host, "http_fetch") == []


# --- SQL --------------------------------------------------------------------------


def test_sql_record_matches_rust() -> None:
    # Rust: SqlBuilder::new("orders").exactly_once().request("UPDATE ..", [SqlValue::Int8(7),
    #   SqlValue::Text("x"), SqlValue::Null, SqlValue::Boolean(true), SqlValue::Float8(1.5)])
    req = SqlBuilder("orders").exactly_once().request("UPDATE t SET a = $2 WHERE id = $1", [7, "x", None, True, 1.5])
    assert req == SqlRequest(
        connection="orders",
        statement="UPDATE t SET a = $2 WHERE id = $1",
        params=[("int8", 7), ("text", "x"), ("null", None), ("boolean", True), ("float8", 1.5)],
        exactly_once=True,
    )
    assert SqlBuilder("db").request("SELECT 1") == SqlRequest("db", "SELECT 1", [], False)


def test_sql_value_mapping() -> None:
    assert sql_value(("numeric", "1.50")) == ("numeric", "1.50")
    assert sql_value(("json", '{"a":1}')) == ("json", '{"a":1}')
    assert sql_value(b"\x00\x01") == ("bytea", b"\x00\x01")
    at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    assert sql_value(at) == ("timestamptz", 1_767_225_600 * 10**9)
    assert sql_value(dt.datetime(2026, 1, 1)) == ("timestamptz", 1_767_225_600 * 10**9)
    with pytest.raises(ValueError):
        sql_value(2**63)
    with pytest.raises(TypeError):
        sql_value(object())


def test_ctx_sql_query(fake_host: FakeHost) -> None:
    result = SqlResult(columns=["n"], rows=[[("int8", 1)]], rows_affected=1)
    fake_host.script_wait([_ok(FIRST_OP, "sql", result)])

    async def body(ctx: Any) -> Any:
        return await ctx.sql("db").exactly_once().query("UPDATE c SET n = n + 1 RETURNING n")

    assert run_in_loop(fake_host, body) == result
    assert _recorded(fake_host, "sql_query") == [SqlRequest("db", "UPDATE c SET n = n + 1 RETURNING n", [], True)]


# --- object store ------------------------------------------------------------------


def test_object_records_match_rust() -> None:
    c = ObjectClient("files")
    assert c.request("get", "a.txt") == ObjectRequest("files", "get", "a.txt", None)
    assert c.request("put", "a.txt", b"x") == ObjectRequest("files", "put", "a.txt", b"x")


def test_ctx_object_ops_and_decoders(fake_host: FakeHost) -> None:
    outputs = [
        b"data",
        b'{"e_tag":"\\"x\\""}',
        b'{"size":3,"last_modified":"2026-01-01T00:00:00Z","e_tag":null,"version":null}',
        b"",
        b"https://x/y?sig=1",
        b"https://x/p?sig=2",
        b"raw",
    ]
    for i, out in enumerate(outputs):
        fake_host.script_wait([_ok(FIRST_OP + i, "object", out)])

    async def body(ctx: Any) -> Any:
        o = ctx.object("files")
        return [
            await o.get("k"),
            await o.put("k", "body"),
            await o.head("k"),
            await o.delete("k"),
            await o.presign_get("k"),
            await o.presign_put("k"),
            await o.raw("list", "k", b"b"),
        ]

    assert run_in_loop(fake_host, body) == [
        b"data",
        ObjectPutInfo(e_tag='"x"'),
        ObjectHead(size=3, last_modified="2026-01-01T00:00:00Z", e_tag=None, version=None),
        None,
        "https://x/y?sig=1",
        "https://x/p?sig=2",
        b"raw",
    ]
    assert _recorded(fake_host, "object_store") == [
        ObjectRequest("files", "get", "k", None),
        ObjectRequest("files", "put", "k", b"body"),
        ObjectRequest("files", "head", "k", None),
        ObjectRequest("files", "delete", "k", None),
        ObjectRequest("files", "presign-get", "k", None),
        ObjectRequest("files", "presign-put", "k", None),
        ObjectRequest("files", "list", "k", b"b"),
    ]


def test_object_decode_errors(fake_host: FakeHost) -> None:
    fake_host.script_wait([_ok(FIRST_OP, "object", b"nope")], [_ok(FIRST_OP + 1, "object", b"\xff")])

    async def body(ctx: Any) -> Any:
        kinds = []
        for call in (lambda: ctx.object("f").head("k"), lambda: ctx.object("f").presign_get("k")):
            try:
                await call()
            except NonRetryable as e:
                kinds.append(e.kind)
        return kinds

    assert run_in_loop(fake_host, body) == ["invalid_json", "protocol"]


# --- idempotency key ----------------------------------------------------------------


def test_ctx_idempotency_key_uses_the_run_id(fake_host: FakeHost) -> None:
    from ostro import idempotency_key_for

    async def body(ctx: Any) -> Any:
        return ctx.idempotency_key("charge")

    key = run_in_loop(fake_host, body)
    assert key == idempotency_key_for(fake_host.run_info.run_id, "charge")
    assert len(key) == 38 and key.startswith("ostro-")
