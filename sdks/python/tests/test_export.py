"""Registry, `run` result mapping, `export()`, and the guest bindings adapter.

The `_bindings` tests import the real componentize-py 0.25.1 generated bindings (pure
Python stubs on the host) and replace their import functions with fakes, so the type
conversions are checked against the generated classes themselves. They are skipped when
the pinned toolchain cannot generate the bindings (no uv / offline cache miss).
"""

from __future__ import annotations

import gc
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import ostro
from ostro import Cancelled, InvalidString, NonRetryable, Retryable, step, workflow
from ostro._host import ErrorInfo
from ostro.export import Continuation, HostWorkflows, WorkflowMeta, describe, run

from conftest import FakeHost, methods

JSON = "application/json"
SDK_ROOT = Path(__file__).resolve().parents[1]


# --- registry -------------------------------------------------------------------------


def test_describe_sorted_and_unique(registry: Any) -> None:
    @workflow(name="zeta", version=3, queue="q", run_timeout=60)
    async def z(ctx, inp): ...

    @workflow
    async def alpha(ctx, inp): ...

    assert describe() == [WorkflowMeta("alpha", 1, None, None), WorkflowMeta("zeta", 3, "q", 60000)]
    with pytest.raises(ValueError, match="duplicate"):

        @workflow(name="alpha")
        async def again(ctx, inp): ...


def test_workflow_name_with_lone_surrogate(registry: Any) -> None:
    with pytest.raises(InvalidString):

        @workflow(name="bad\ud800")
        async def w(ctx, inp): ...


def test_registration_after_freeze(registry: Any) -> None:
    registry.freeze()
    with pytest.raises(RuntimeError, match="after ostro.export"):

        @workflow
        async def late(ctx, inp): ...


# --- run ------------------------------------------------------------------------------


def test_run_completes_with_json(registry: Any, fake_host: FakeHost) -> None:
    @step
    def greet(name: str) -> str:
        return f"hello, {name}"

    @workflow
    async def hello(ctx, name):
        return {"greeting": await greet(name), "n": None}

    assert run("hello", (JSON, b'"ostro"')) == ("completed", (JSON, '{"greeting":"hello, ostro","n":null}'.encode()))
    assert methods(fake_host) == ["begin_step", "step_complete"]


def test_run_empty_input_is_none_and_sync_workflows(registry: Any, fake_host: FakeHost) -> None:
    @workflow
    def echo(ctx, inp):
        return {"input": inp}

    assert run("echo", (JSON, b"")) == ("completed", (JSON, b'{"input":null}'))


def test_run_unknown_workflow(registry: Any, fake_host: FakeHost) -> None:
    assert run("nope", (JSON, b"")) == (
        "err",
        ("non-retryable", ErrorInfo("unknown_workflow", "unknown workflow 'nope'")),
    )
    assert fake_host.calls == []


@pytest.mark.parametrize(
    "exc, failure",
    [
        (Cancelled(), ("cancelled", None)),
        (NonRetryable("declined", "card"), ("non-retryable", ErrorInfo("declined", "card"))),
        (Retryable("busy", "later"), ("retryable", ErrorInfo("busy", "later"))),
        (KeyError("k"), ("non-retryable", ErrorInfo("python_exception", "KeyError: 'k'"))),
        (ValueError("bad"), ("non-retryable", ErrorInfo("python_exception", "ValueError: bad"))),
    ],
)
def test_run_failure_mapping(registry: Any, fake_host: FakeHost, exc: Exception, failure: Any) -> None:
    @workflow
    async def fails(ctx, inp):
        raise exc

    assert run("fails", (JSON, b"")) == ("err", failure)


def test_run_deadlock_is_sdk_protocol(registry: Any, fake_host: FakeHost) -> None:
    import asyncio

    @workflow
    async def stuck(ctx, inp):
        await asyncio.get_running_loop().create_future()

    tag, (case, info) = run("stuck", (JSON, b""))
    assert (tag, case, info.kind) == ("err", "non-retryable", "sdk_protocol")


def test_run_continue_as_new(registry: Any, fake_host: FakeHost) -> None:
    @workflow
    async def loop_wf(ctx, n):
        try:
            ctx.continue_as_new(n + 1, workflow="other", version=2)
        except Exception:  # user code cannot swallow it
            raise AssertionError("caught") from None

    assert run("loop_wf", (JSON, b"1")) == ("continue-as-new", Continuation((JSON, b"2"), "other", 2))


def test_run_invalid_input(registry: Any, fake_host: FakeHost) -> None:
    @workflow
    async def w(ctx, inp):
        return inp

    tag, (case, info) = run("w", (JSON, b"{nope"))
    assert (tag, case, info.kind) == ("err", "non-retryable", "invalid_json")


def test_closed_after_settle_leftover_task(registry: Any, fake_host: FakeHost) -> None:
    import asyncio

    kept: list[Any] = []

    @workflow
    async def w(ctx, inp):
        async def leftover():
            ctx.sleep(1)

        kept.append((ctx, asyncio.ensure_future(leftover())))
        return 1

    assert run("w", (JSON, b""))[0] == "completed"
    ctx, task = kept[0]
    with pytest.raises(ostro.ProtocolError):
        ctx.sleep(1)
    task.get_coro().close()
    assert fake_host.calls == []


def test_export_on_the_host(registry: Any, fake_host: FakeHost) -> None:
    @workflow
    async def w(ctx, inp):
        return inp

    namespace: dict[str, Any] = {"ostro": ostro}
    try:
        exec("cls = ostro.export()", namespace)
        assert namespace["Workflows"] is namespace["cls"] is HostWorkflows
        assert registry.frozen
        assert gc.get_freeze_count() > 0
        wfs = namespace["Workflows"]()
        assert wfs.describe() == [WorkflowMeta("w", 1)]
        assert wfs.run("w", (JSON, b"[1]")) == ("completed", (JSON, b"[1]"))
    finally:
        gc.unfreeze()


# --- the guest bindings adapter ---------------------------------------------------------


@pytest.fixture(scope="module")
def bindings_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv not found: cannot generate componentize-py bindings")
    out = tmp_path_factory.mktemp("bindings")
    cmd = [
        uv, "tool", "run", "--from", "componentize-py==0.25.1", "componentize-py",
        "-d", str(SDK_ROOT / "ostro" / "wit"), "-w", "ostro:runtime/workflow@0.1.0",
        "bindings", str(out),
    ]  # fmt: skip
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.TimeoutExpired) as e:
        pytest.skip(f"componentize-py bindings generation failed: {e}")
    if proc.returncode != 0 or not (out / "wit_world" / "__init__.py").exists():
        pytest.skip(f"componentize-py bindings generation failed: {proc.stderr[-500:]}")
    return out


_GENERATED = ("wit_world", "componentize_py_types", "ostro._bindings")


def _purge() -> None:
    for name in list(sys.modules):
        if any(name == g or name.startswith(g + ".") for g in _GENERATED):
            del sys.modules[name]
    # `from ostro import _bindings` would return a stale package attribute.
    if hasattr(ostro, "_bindings"):
        delattr(ostro, "_bindings")


@pytest.fixture
def gen(bindings_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    _purge()
    monkeypatch.syspath_prepend(str(bindings_dir))
    import componentize_py_types
    import wit_world.exports.workflows
    import wit_world.imports.context
    import wit_world.imports.effects
    import wit_world.imports.journal
    import wit_world.imports.log
    import wit_world.imports.types

    from ostro import _bindings

    ns = type("Gen", (), {})()
    ns.types = wit_world.imports.types
    ns.context = wit_world.imports.context
    ns.journal = wit_world.imports.journal
    ns.effects = wit_world.imports.effects
    ns.log = wit_world.imports.log
    ns.wf = wit_world.exports.workflows
    ns.Ok = componentize_py_types.Ok
    ns.Err = componentize_py_types.Err
    ns.b = _bindings
    yield ns
    _purge()


class FakeWitStep:
    def __init__(self, acks: list[Any]):
        self.acks = acks
        self.completed: list[Any] = []
        self.exited = False

    def seq(self) -> int:
        return 3

    def complete(self, outcome: Any) -> Any:
        self.completed.append(outcome)
        return self.acks.pop(0)

    def __exit__(self, *a: Any) -> None:
        self.exited = True


def test_bindings_host_conversions(gen: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    t, j, e, c = gen.types, gen.journal, gen.effects, gen.context
    host = gen.b.BindingsHost()
    calls: list[tuple] = []

    monkeypatch.setattr(c, "now", lambda: t.Datetime(seconds=5, nanoseconds=7))
    assert host.now_nanos() == 5_000_000_007

    replayed = j.StepStart_Replayed(gen.Ok(t.Payload(JSON, b"1")))
    monkeypatch.setattr(j, "begin_step", lambda name, opts: calls.append((name, opts)) or replayed)
    assert host.begin_step("s", ostro._host.StepOptions(timeout_ms=5)) == ("replayed", ("ok", (JSON, b"1")))
    assert calls[-1] == ("s", t.StepOptions(retry=None, timeout_ms=5, fuel=None))
    failed = j.StepStart_Replayed(gen.Err(t.Failure_NonRetryable(t.ErrorInfo("k", "m", None))))
    monkeypatch.setattr(j, "begin_step", lambda name, opts: failed)
    assert host.begin_step("s", ostro._host.StepOptions()) == ("replayed", ("err", ("non-retryable", ErrorInfo("k", "m"))))

    step_res = FakeWitStep([j.CompletionAck_RetryNow(), j.CompletionAck_Recorded()])
    assert host.step_complete(step_res, ("err", ("retryable", ErrorInfo("x", "y")))) == "retry-now"
    assert not step_res.exited
    assert host.step_complete(step_res, ("ok", (JSON, b"2"))) == "recorded"
    assert step_res.exited
    assert step_res.completed == [
        gen.Err(t.Failure_Retryable(t.ErrorInfo("x", "y", None))),
        gen.Ok(t.Payload(JSON, b"2")),
    ]

    monkeypatch.setattr(e, "sleep_until", lambda at: calls.append(("sleep_until", at)) or 9)
    assert host.sleep_until(3 * 10**9 + 4) == 9
    assert calls[-1] == ("sleep_until", t.Datetime(3, 4))

    completions = [
        e.Completion(16, gen.Ok(e.OpOutput_Signal(t.Payload(JSON, b'"s"')))),
        e.Completion(17, gen.Err(t.Failure_Cancelled())),
        e.Completion(18, gen.Ok(e.OpOutput_Timer())),
        e.Completion(19, gen.Ok(e.OpOutput_Sql(e.SqlResult(["a"], [[e.SqlValue_Int8(1), e.SqlValue_Null()]], 1)))),
    ]
    monkeypatch.setattr(e, "wait", lambda ops, mode: calls.append(("wait", ops, mode)) or completions)
    assert host.wait([16, 17, 18, 19], True) == [
        (16, ("ok", ("signal", (JSON, b'"s"')))),
        (17, ("err", ("cancelled", None))),
        (18, ("ok", ("timer", None))),
        (19, ("ok", ("sql", ostro._host.SqlResult(["a"], [[("int8", 1), ("null", None)]], 1)))),
    ]
    assert calls[-1] == ("wait", [16, 17, 18, 19], e.WaitMode.ANY)

    monkeypatch.setattr(gen.log, "emit", lambda lvl, msg, fields: calls.append(("emit", lvl, msg, fields)))
    host.log_emit("warn", "m", [("k", "v")])
    assert calls[-1] == ("emit", gen.log.Level.WARN, "m", [("k", "v")])

    monkeypatch.setattr(
        e, "start_child", lambda w, inp, opts: calls.append(("start_child", w, inp, opts)) or 20
    )
    assert host.start_child("leaf", (JSON, b"{}"), ostro._host.ChildOptions(detached=True)) == 20
    assert calls[-1] == ("start_child", "leaf", t.Payload(JSON, b"{}"), e.ChildOptions(None, None, True, None))


def test_bindings_workflows_export(gen: Any, registry: Any, fake_host: FakeHost) -> None:
    @workflow(name="ok_wf", queue="q")
    async def ok_wf(ctx, inp):
        return {"in": inp}

    @workflow(name="bad_wf")
    async def bad_wf(ctx, inp):
        raise NonRetryable("declined", "no")

    @workflow(name="can_wf")
    async def can_wf(ctx, inp):
        ctx.continue_as_new([inp])

    cls = gen.b.make_workflows_class(describe, run)
    assert cls.__name__ == "Workflows"
    assert sys.modules["wit_world"].exports.Workflows in cls.__mro__
    w = cls()
    t, wf = gen.types, gen.wf
    assert w.describe() == [
        wf.WorkflowMeta("bad_wf", 1, None, None),
        wf.WorkflowMeta("can_wf", 1, None, None),
        wf.WorkflowMeta("ok_wf", 1, "q", None),
    ]
    assert w.run("ok_wf", t.Payload(JSON, b"5")) == wf.RunResult_Completed(t.Payload(JSON, b'{"in":5}'))
    assert w.run("can_wf", t.Payload(JSON, b"5")) == wf.RunResult_ContinueAsNew(
        wf.Continuation(t.Payload(JSON, b"[5]"), None, None)
    )
    with pytest.raises(gen.Err) as e:
        w.run("bad_wf", t.Payload(JSON, b""))
    assert e.value.value == t.Failure_NonRetryable(t.ErrorInfo("declined", "no", None))


def test_bindings_end_to_end_activation(gen: Any, registry: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A workflow with a step and a durable sleep, entirely over the generated bindings."""
    t, j, e, c = gen.types, gen.journal, gen.effects, gen.context
    trace: list[Any] = []
    wit_step = FakeWitStep([j.CompletionAck_Recorded()])
    monkeypatch.setattr(j, "begin_step", lambda name, opts: trace.append(("begin_step", name)) or j.StepStart_Execute(wit_step))
    monkeypatch.setattr(c, "now", lambda: t.Datetime(100, 0))
    monkeypatch.setattr(c, "random_u64", lambda: trace.append("random_u64") or 2**63)
    monkeypatch.setattr(e, "sleep", lambda ms: trace.append(("sleep", ms)) or 16)
    monkeypatch.setattr(e, "cancellation", lambda: 2**64 - 1)
    monkeypatch.setattr(
        e, "wait", lambda ops, mode: trace.append(("wait", ops)) or [e.Completion(16, gen.Ok(e.OpOutput_Timer()))]
    )

    @step
    def greet(name: str) -> str:
        return f"hi {name}"

    @workflow
    async def hello(ctx, name):
        g = await greet(name)
        await ctx.sleep(1)
        return {"g": g, "f": ctx.random_f64()}

    previous = ostro._host.install(gen.b.BindingsHost())
    try:
        result = gen.b.make_workflows_class(describe, run)().run("hello", t.Payload(JSON, b'"x"'))
    finally:
        ostro._host.install(previous)
    assert result == gen.wf.RunResult_Completed(t.Payload(JSON, b'{"g":"hi x","f":0.5}'))
    assert trace == [("begin_step", "greet"), ("sleep", 1000), ("wait", [16, 2**64 - 1]), "random_u64"]
    assert wit_step.completed == [gen.Ok(t.Payload(JSON, b'"hi x"'))]
