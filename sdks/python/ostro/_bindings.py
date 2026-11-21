"""The real host: componentize-py's generated ``wit_world`` bindings adapted to `Host`.

The **only** module that imports the generated bindings (``wit_world``,
``componentize_py_types``). It is imported by `ostro.export()` at app import time, which
is inside the pre-init snapshot: a module imported for the first time during ``run``
traps under ``--stub-wasi`` (S9), so nothing here imports lazily.

Type mapping (see `ostro._host` for the SDK side): WIT variants ↔ tagged tuples,
``result`` ↔ ``("ok", v)`` / ``("err", failure)``, ``datetime`` ↔ integer nanoseconds,
records ↔ the frozen dataclasses of `ostro._host`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import wit_world
from componentize_py_types import Err, Ok
from wit_world.exports import workflows as _wf
from wit_world.imports import context as _context
from wit_world.imports import effects as _effects
from wit_world.imports import journal as _journal
from wit_world.imports import log as _log
from wit_world.imports import types as _types

from . import _host as H

__all__ = ["BindingsHost", "make_workflows_class"]

_NS = 1_000_000_000


# --- conversions -----------------------------------------------------------------------


def _dt_to_ns(d: Any) -> int:
    return d.seconds * _NS + d.nanoseconds


def _ns_to_dt(ns: int) -> Any:
    if ns < 0:
        ns = 0
    return _types.Datetime(seconds=ns // _NS, nanoseconds=ns % _NS)


def _payload_out(p: H.Payload) -> Any:
    content_type, data = p
    return _types.Payload(content_type=content_type, data=bytes(data))


def _payload_in(p: Any) -> H.Payload:
    return (p.content_type, bytes(p.data))


def _opt_payload_in(p: Any) -> H.Payload | None:
    return None if p is None else _payload_in(p)


def _error_info_out(i: H.ErrorInfo) -> Any:
    details = None if i.details is None else _payload_out(i.details)
    return _types.ErrorInfo(kind=i.kind, message=i.message, details=details)


def _failure_out(f: H.Failure) -> Any:
    case, info = f
    if case == "retryable":
        return _types.Failure_Retryable(_error_info_out(info))
    if case == "non-retryable":
        return _types.Failure_NonRetryable(_error_info_out(info))
    if case == "timeout":
        return _types.Failure_Timeout()
    if case == "cancelled":
        return _types.Failure_Cancelled()
    raise ValueError(f"unknown failure case {case!r}")


def _failure_in(f: Any) -> H.Failure:
    if isinstance(f, _types.Failure_Retryable):
        v = f.value
        return ("retryable", H.ErrorInfo(v.kind, v.message, _opt_payload_in(v.details)))
    if isinstance(f, _types.Failure_NonRetryable):
        v = f.value
        return ("non-retryable", H.ErrorInfo(v.kind, v.message, _opt_payload_in(v.details)))
    if isinstance(f, _types.Failure_Timeout):
        return ("timeout", None)
    return ("cancelled", None)


def _retry_out(r: H.RetryPolicy | None) -> Any:
    if r is None:
        return None
    return _types.RetryPolicy(
        max_attempts=r.max_attempts,
        initial_interval_ms=r.initial_interval_ms,
        max_interval_ms=r.max_interval_ms,
        backoff_coefficient=r.backoff_coefficient,
        non_retryable_kinds=list(r.non_retryable_kinds),
    )


def _headers_out(hs: list[H.Header]) -> list[Any]:
    return [_effects.Header(name=h.name, value=h.value) for h in hs]


def _headers_in(hs: list[Any]) -> list[H.Header]:
    return [H.Header(name=h.name, value=h.value) for h in hs]


_SQL_OUT: dict[str, Any] = {
    "boolean": _effects.SqlValue_Boolean,
    "int8": _effects.SqlValue_Int8,
    "float8": _effects.SqlValue_Float8,
    "numeric": _effects.SqlValue_Numeric,
    "text": _effects.SqlValue_Text,
    "bytea": _effects.SqlValue_Bytea,
    "json": _effects.SqlValue_Json,
}
_SQL_IN: dict[type, str] = {cls: case for case, cls in _SQL_OUT.items()}


def _sql_value_out(v: H.SqlValue) -> Any:
    case, value = v
    if case == "null":
        return _effects.SqlValue_Null()
    if case == "timestamptz":
        return _effects.SqlValue_Timestamptz(_ns_to_dt(value))
    return _SQL_OUT[case](value)


def _sql_value_in(v: Any) -> H.SqlValue:
    if isinstance(v, _effects.SqlValue_Null):
        return ("null", None)
    if isinstance(v, _effects.SqlValue_Timestamptz):
        return ("timestamptz", _dt_to_ns(v.value))
    return (_SQL_IN[type(v)], v.value)


def _op_output_in(o: Any) -> H.OpOutput:
    if isinstance(o, _effects.OpOutput_Http):
        r = o.value
        return ("http", H.HttpResponse(status=r.status, headers=_headers_in(r.headers), body=bytes(r.body)))
    if isinstance(o, _effects.OpOutput_Sql):
        r = o.value
        rows = [[_sql_value_in(v) for v in row] for row in r.rows]
        return ("sql", H.SqlResult(columns=list(r.columns), rows=rows, rows_affected=r.rows_affected))
    if isinstance(o, _effects.OpOutput_Object):
        return ("object", bytes(o.value))
    if isinstance(o, _effects.OpOutput_Timer):
        return ("timer", None)
    if isinstance(o, _effects.OpOutput_Signal):
        return ("signal", _payload_in(o.value))
    if isinstance(o, _effects.OpOutput_Child):
        return ("child", _payload_in(o.value))
    if isinstance(o, _effects.OpOutput_Token):
        return ("token", _payload_in(o.value))
    return ("cancel-requested", None)


def _result_in(r: Any, ok: Callable[[Any], Any]) -> H.Outcome:
    if isinstance(r, Ok):
        return ("ok", ok(r.value))
    return ("err", _failure_in(r.value))


def _outcome_out(o: H.Outcome) -> Any:
    tag, value = o
    if tag == "ok":
        return Ok(_payload_out(value))
    return Err(_failure_out(value))


_LEVELS = {
    "trace": _log.Level.TRACE,
    "debug": _log.Level.DEBUG,
    "info": _log.Level.INFO,
    "warn": _log.Level.WARN,
    "error": _log.Level.ERROR,
}


# --- the host --------------------------------------------------------------------------


class BindingsHost:
    """`ostro._host.Host` over the generated bindings (guest only)."""

    # context
    def info(self) -> H.RunInfo:
        i = _context.info()
        return H.RunInfo(
            run_id=i.run_id,
            workflow=i.workflow,
            workflow_version=i.workflow_version,
            attempt=i.attempt,
            module_hash=i.module_hash,
            tenant=i.tenant,
            parent_run_id=i.parent_run_id,
            deadline=None if i.deadline is None else _dt_to_ns(i.deadline),
            continued_from=i.continued_from,
        )

    def now_nanos(self) -> int:
        return _dt_to_ns(_context.now())

    def random_u64(self) -> int:
        return _context.random_u64()

    def random_bytes(self, n: int) -> bytes:
        return bytes(_context.random_bytes(n))

    def uuid_v7(self) -> str:
        return _context.uuid_v7()

    def is_replaying(self) -> bool:
        return _context.is_replaying()

    def patched(self, change_id: str) -> bool:
        return _context.patched(change_id)

    def set_status(self, json: str) -> None:
        _context.set_status(json)

    def continue_as_new_suggested(self) -> bool:
        return _context.continue_as_new_suggested()

    # journal
    def begin_step(self, name: str, opts: H.StepOptions) -> H.StepStart:
        wit_opts = _types.StepOptions(retry=_retry_out(opts.retry), timeout_ms=opts.timeout_ms, fuel=opts.fuel)
        start = _journal.begin_step(name, wit_opts)
        if isinstance(start, _journal.StepStart_Replayed):
            return ("replayed", _result_in(start.value, _payload_in))
        return ("execute", start.value)

    def step_seq(self, handle: Any) -> int:
        return handle.seq()

    def step_complete(self, handle: Any, outcome: H.Outcome) -> H.CompletionAck:
        ack = handle.complete(_outcome_out(outcome))
        if isinstance(ack, _journal.CompletionAck_RetryNow):
            return "retry-now"
        # Recorded: the SDK never uses the handle again; release the resource now.
        handle.__exit__(None, None, None)
        return "recorded"

    # effects
    def http_fetch(self, req: H.HttpRequest) -> int:
        return _effects.http_fetch(
            _effects.HttpRequest(
                method=req.method,
                url=req.url,
                headers=_headers_out(req.headers),
                body=None if req.body is None else bytes(req.body),
                timeout_ms=req.timeout_ms,
                idempotency_key=req.idempotency_key,
                retry=_retry_out(req.retry),
                redact=list(req.redact),
            )
        )

    def sql_query(self, req: H.SqlRequest) -> int:
        return _effects.sql_query(
            _effects.SqlRequest(
                connection=req.connection,
                statement=req.statement,
                params=[_sql_value_out(v) for v in req.params],
                exactly_once=req.exactly_once,
            )
        )

    def object_store(self, req: H.ObjectRequest) -> int:
        return _effects.object_store(
            _effects.ObjectRequest(
                connection=req.connection,
                op=req.op,
                key=req.key,
                body=None if req.body is None else bytes(req.body),
            )
        )

    def sleep(self, duration_ms: int) -> int:
        return _effects.sleep(duration_ms)

    def sleep_until(self, at_nanos: int) -> int:
        return _effects.sleep_until(_ns_to_dt(at_nanos))

    def wait_signal(self, name: str, timeout_ms: int | None) -> int:
        return _effects.wait_signal(name, timeout_ms)

    def start_child(self, workflow: str, input: H.Payload, opts: H.ChildOptions) -> int:
        return _effects.start_child(
            workflow,
            _payload_out(input),
            _effects.ChildOptions(
                queue=opts.queue,
                version=opts.version,
                detached=opts.detached,
                run_timeout_ms=opts.run_timeout_ms,
            ),
        )

    def create_await_token(self, timeout_ms: int | None) -> H.AwaitToken:
        t = _effects.create_await_token(timeout_ms)
        return H.AwaitToken(op=t.op, token=t.token)

    def cancellation(self) -> int:
        return _effects.cancellation()

    def cancel_op(self, op: int) -> None:
        _effects.cancel_op(op)

    def wait(self, ops: list[int], any: bool) -> list[H.Completion]:
        mode = _effects.WaitMode.ANY if any else _effects.WaitMode.ALL
        return [(c.op, _result_in(c.outcome, _op_output_in)) for c in _effects.wait(list(ops), mode)]

    # log
    def log_emit(self, level: H.LogLevel, message: str, fields: list[tuple[str, str]]) -> None:
        _log.emit(_LEVELS[level], message, list(fields))


# --- the export ------------------------------------------------------------------------


def make_workflows_class(describe: Callable[[], list[Any]], run: Callable[[str, H.Payload], Any]) -> type:
    """``class Workflows(wit_world.exports.Workflows)`` over the SDK's `describe`/`run`."""

    class Workflows(wit_world.exports.Workflows):
        def describe(self) -> list[_wf.WorkflowMeta]:
            return [
                _wf.WorkflowMeta(
                    name=m.name,
                    version=m.version,
                    default_queue=m.default_queue,
                    run_timeout_ms=m.run_timeout_ms,
                )
                for m in describe()
            ]

        def run(self, name: str, input: Any) -> Any:
            tag, value = run(name, _payload_in(input))
            if tag == "completed":
                return _wf.RunResult_Completed(_payload_out(value))
            if tag == "continue-as-new":
                return _wf.RunResult_ContinueAsNew(
                    _wf.Continuation(input=_payload_out(value.input), workflow=value.workflow, version=value.version)
                )
            raise Err(_failure_out(value))

    Workflows.__module__ = "app"
    return Workflows
