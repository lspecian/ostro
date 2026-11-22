"""Unit tests against a local http.server stub (no Ostro server needed)."""

from __future__ import annotations

import hashlib
import inspect
import json
import socket
import urllib.parse

import pytest

import ostro_client
from ostro_client import Client, OstroError, ROUTES, parse_duration
from ostro_client import client as client_mod


def body(req):
    return json.loads(req["body"]) if req["body"] else None


def split(req):
    u = urllib.parse.urlsplit(req["path"])
    return u.path, urllib.parse.parse_qs(u.query)


def test_start_run_posts_json_to_the_workflow_runs_path(stub):
    stub.reply(201, {"run_id": "r1", "created": True})
    c = Client(stub.url)
    out = c.start_run("order", {"id": 7}, idempotency_key="k1", tags=["a=b"], priority=3)
    assert out == {"run_id": "r1", "created": True}
    req = stub.last
    assert req["method"] == "POST"
    assert split(req)[0] == "/v1/workflows/order/runs"
    assert req["headers"]["Content-Type"] == "application/json"
    assert body(req) == {"input": {"id": 7}, "idempotency_key": "k1", "tags": ["a=b"], "priority": 3}


def test_tenant_qualified_workflow_is_one_encoded_segment(stub):
    c = Client(stub.url)
    c.start_run("acme/order")
    assert split(stub.last)[0] == "/v1/workflows/acme%2Forder/runs"
    c.signal("r 1", "go/now", {"ok": True}, request_id="q")
    assert split(stub.last)[0] == "/v1/runs/r%201/signals/go%2Fnow"
    assert body(stub.last) == {"payload": {"ok": True}, "request_id": "q"}


def test_result_sends_wait_and_extends_the_socket_timeout(stub, monkeypatch):
    seen = []
    real = client_mod.urllib.request.urlopen

    def spy(req, timeout=None):
        seen.append(timeout)
        return real(req, timeout=timeout)

    monkeypatch.setattr(client_mod.urllib.request, "urlopen", spy)
    stub.reply(200, {"run_id": "r1", "completed": True, "status": "completed", "result": 1})
    c = Client(stub.url, timeout=5)
    r = c.result("r1", wait="30s")
    assert r["result"] == 1
    path, q = split(stub.last)
    assert path == "/v1/runs/r1/result"
    assert q["wait"] == ["30000ms"]
    assert seen[0] >= 30 + 5


def test_result_without_wait_polls_once(stub):
    stub.reply(200, {"run_id": "r1", "completed": False})
    r = Client(stub.url).result("r1")
    assert r["completed"] is False
    assert split(stub.last)[1]["wait"] == ["0ms"]
    assert len(stub.requests) == 1


def test_error_body_maps_to_ostro_error(stub):
    stub.reply(409, {"error": {"code": "run_terminal", "message": "run is completed", "details": {"status": "completed"}}})
    with pytest.raises(OstroError) as ei:
        Client(stub.url).cancel("r1", "x")
    e = ei.value
    assert (e.status, e.code, e.message) == (409, "run_terminal", "run is completed")
    assert e.details == {"status": "completed"}
    assert body(stub.last) == {"reason": "x"}


def test_non_json_error_is_http_status_code(stub):
    stub.reply(502, b"bad gateway")
    with pytest.raises(OstroError) as ei:
        Client(stub.url).get_run("r1")
    assert (ei.value.status, ei.value.code, ei.value.message) == (502, "http_502", "bad gateway")


def test_details_reason(stub):
    stub.reply(409, {"error": {"code": "failed_precondition", "message": "off", "details": {"reason": "time_skipping_disabled"}}})
    with pytest.raises(OstroError) as ei:
        Client(stub.url).dev_time()
    assert ei.value.reason == "time_skipping_disabled"


def test_connection_refused_is_unreachable():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with pytest.raises(OstroError) as ei:
        Client(f"http://127.0.0.1:{port}", timeout=2).get_run("r1")
    assert ei.value.code == "unreachable"
    assert ei.value.status is None


def test_timeout_is_timeout(stub):
    stub.delay = 1.0
    with pytest.raises(OstroError) as ei:
        Client(stub.url, timeout=0.2).get_run("r1")
    assert ei.value.code == "timeout"


def test_upload_module_puts_wasm_under_its_sha(stub):
    wasm = b"\0asm\x01\0\0\0"
    stub.reply(201, {"created": True, "module": {"sha256": "x"}})
    Client(stub.url).upload_module(wasm, name="m", tenant="acme")
    req = stub.last
    path, q = split(req)
    assert req["method"] == "PUT"
    assert path == "/v1/modules/" + hashlib.sha256(wasm).hexdigest()
    assert q == {"name": ["m"], "tenant": ["acme"]}
    assert req["headers"]["Content-Type"] == "application/wasm"
    assert req["body"] == wasm


def test_list_runs_and_history_query(stub):
    c = Client(stub.url)
    c.list_runs(workflow="w", tags=["a", "b"], limit=5)
    path, q = split(stub.last)
    assert path == "/v1/runs"
    assert q == {"workflow": ["w"], "tag": ["a", "b"], "limit": ["5"]}
    c.history("r1", from_seq=3, limit=10)
    assert split(stub.last) == ("/v1/runs/r1/history", {"from_seq": ["3"], "limit": ["10"]})


def test_dev_skip_and_replay_bodies(stub):
    c = Client(stub.url)
    c.dev_skip("24h")
    assert body(stub.last) == {"by": "24h"}
    c.dev_skip(90)
    assert body(stub.last) == {"by": "90000ms"}
    c.dev_skip(to_ms=5)
    assert body(stub.last) == {"to_ms": 5}
    with pytest.raises(ValueError):
        c.dev_skip()
    c.replay("abc", runs=["r1"], allow_missing=True)
    assert split(stub.last)[0] == "/v1/modules/abc/replay"
    assert body(stub.last) == {"runs": ["r1"], "allow_missing": True}
    c.replay("abc", sample=5, workflow="w")
    assert body(stub.last) == {"sample": 5, "workflow": "w"}


def test_parse_duration():
    assert parse_duration("30s") == 30
    assert parse_duration("500ms") == 0.5
    assert parse_duration("72h") == 72 * 3600
    assert parse_duration(1.5) == 1.5
    with pytest.raises(ValueError):
        parse_duration("soon")


def test_server_env_default(monkeypatch):
    monkeypatch.setenv("OSTRO_SERVER", "127.0.0.1:9")
    assert Client().server == "http://127.0.0.1:9"
    monkeypatch.delenv("OSTRO_SERVER")
    assert Client().server == ostro_client.DEFAULT_SERVER


# Every public method with sample arguments: its (method, template) must be in ROUTES.
CALLS = {
    "upload_module": ((b"x",), {}),
    "workflows": ((), {}),
    "activate": (("w", 1), {}),
    "start_run": (("w",), {}),
    "get_run": (("r",), {}),
    "list_runs": ((), {}),
    "history": (("r",), {}),
    "result": (("r",), {}),
    "signal": (("r", "s"), {}),
    "cancel": (("r",), {}),
    "terminate": (("r",), {}),
    "retry": (("r",), {}),
    "complete_token": (("t",), {}),
    "dev_time": ((), {}),
    "dev_skip": (("1h",), {}),
    "replay": (("sha",), {"runs": ["r"]}),
}


def test_every_method_route_is_declared(monkeypatch):
    public = {n for n, f in inspect.getmembers(Client, inspect.isfunction) if not n.startswith("_")}
    assert public == set(CALLS), "update CALLS for new methods"
    used = []

    def record(self, method, template, *a, **kw):
        used.append((method, template))
        return {"workflows": [], "completed": True}

    monkeypatch.setattr(Client, "_request", record)
    c = Client("http://127.0.0.1:1")
    for name, (args, kw) in CALLS.items():
        n = len(used)
        getattr(c, name)(*args, **kw)
        assert len(used) > n, name
    routes = set(ROUTES)
    for r in used:
        assert r in routes, r
    assert set(used) == routes, "ROUTES lists a route no method calls"
