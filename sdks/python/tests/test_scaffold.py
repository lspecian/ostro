"""Wave-0 scaffold tests: the host contract, the fake host and the vendored WIT copy."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

import ostro
from ostro import _host
from ostro._host import CANCELLATION_OP, ChildOptions, Host, StepOptions

from conftest import FIRST_OP, FakeHost

SDK_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = SDK_ROOT.parents[1]
WIT_SOURCE = REPO_ROOT / "wit" / "ostro-runtime-0.1.0" / "runtime.wit"
WIT_COPY = SDK_ROOT / "ostro" / "wit" / "runtime.wit"


def test_version() -> None:
    assert ostro.__version__ == "0.1.0"


def test_fake_host_satisfies_the_host_protocol(fake_host: FakeHost) -> None:
    assert isinstance(fake_host, Host)


def test_host_mirrors_every_wit_import_function() -> None:
    """Every function of the imported WIT interfaces has a `Host` method."""
    text = WIT_SOURCE.read_text(encoding="utf-8")
    expected: set[str] = set()
    for iface in ("context", "journal", "effects", "log"):
        body = re.search(r"^interface " + iface + r" \{(.*?)^\}", text, re.S | re.M)
        assert body is not None, iface
        for name in re.findall(r"^\s+([a-z][a-z0-9-]*): func\(", body.group(1), re.M):
            expected.add(name.replace("-", "_"))
    # Renamed to fit Python: datetime as nanoseconds, the step resource's methods,
    # `log.emit` namespaced.
    renames = {"now": "now_nanos", "seq": "step_seq", "complete": "step_complete", "emit": "log_emit"}
    expected = {renames.get(n, n) for n in expected}
    methods = {n for n, _ in inspect.getmembers(Host, inspect.isfunction) if not n.startswith("_")}
    assert expected == methods


def test_install_current_round_trip() -> None:
    first, second = FakeHost(), FakeHost()
    previous = _host.install(first)
    try:
        assert _host.current() is first
        assert _host.install(second) is first
        assert _host.current() is second
    finally:
        _host.install(previous)


def test_current_without_a_host_needs_the_guest_bindings() -> None:
    previous = _host.install(None)
    try:
        with pytest.raises(NotImplementedError, match="09-08"):
            _host.current()
    finally:
        _host.install(previous)


def test_fixture_installs_the_fake_host(fake_host: FakeHost) -> None:
    assert _host.current() is fake_host


def test_fake_host_records_calls_and_allocates_ops(fake_host: FakeHost) -> None:
    h = _host.current()
    a = h.sleep(1000)
    b = h.wait_signal("go", None)
    c = h.start_child("leaf", ("application/json", b"{}"), ChildOptions())
    token = h.create_await_token(5000)
    assert (a, b, c, token.op) == (FIRST_OP, FIRST_OP + 1, FIRST_OP + 2, FIRST_OP + 3)
    assert token.token == f"tok-{FIRST_OP + 3}"
    assert h.cancellation() == CANCELLATION_OP == 2**64 - 1
    h.cancel_op(b)
    h.log_emit("info", "hello", [("k", "v")])
    assert [m for m, _ in fake_host.calls] == [
        "sleep",
        "wait_signal",
        "start_child",
        "create_await_token",
        "cancellation",
        "cancel_op",
        "log_emit",
    ]
    assert fake_host.calls[0] == ("sleep", (1000,))
    assert fake_host.cancelled_ops == [b]
    assert fake_host.logs == [("info", "hello", [("k", "v")])]
    assert fake_host.ops_issued() == [a, b, c, token.op]


def test_fake_host_scripted_wait(fake_host: FakeHost) -> None:
    op = fake_host.sleep(10)
    fake_host.script_wait([(op, ("ok", ("timer", None)))])
    assert fake_host.wait([op, CANCELLATION_OP], True) == [(op, ("ok", ("timer", None)))]
    assert fake_host.calls[-1] == ("wait", ([op, CANCELLATION_OP], True))
    with pytest.raises(AssertionError, match="unscripted wait"):
        fake_host.wait([op], False)


def test_fake_host_steps(fake_host: FakeHost) -> None:
    replayed = ("replayed", ("ok", ("application/json", b"42")))
    fake_host.script_begin_step(replayed)
    assert fake_host.begin_step("a", StepOptions()) == replayed
    kind, handle = fake_host.begin_step("b", StepOptions(timeout_ms=5))
    assert kind == "execute"
    assert fake_host.step_seq(handle) == 1
    fake_host.script_step_ack("retry-now")
    assert fake_host.step_complete(handle, ("ok", ("application/json", b"1"))) == "retry-now"
    assert fake_host.step_complete(handle, ("ok", ("application/json", b"1"))) == "recorded"


def test_fake_host_context_reads(fake_host: FakeHost) -> None:
    assert fake_host.info().workflow == "test"
    assert fake_host.random_u64() != fake_host.random_u64()
    assert len(fake_host.random_bytes(16)) == 16
    assert fake_host.patched("p1") is True
    fake_host.patches["p2"] = False
    assert fake_host.patched("p2") is False
    fake_host.set_status('{"s":1}')
    assert fake_host.statuses == ['{"s":1}']


def test_wit_copy_is_byte_identical() -> None:
    if not WIT_COPY.exists():
        pytest.skip("ostro/wit/runtime.wit is copied by 09-02 and is not committed yet")
    assert WIT_COPY.read_bytes() == WIT_SOURCE.read_bytes()
