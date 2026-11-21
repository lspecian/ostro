"""Spike S9 Python guest (componentize-py 0.25.1, --stub-wasi). Throwaway.

Raw bindings against the real `ostro:runtime/workflow@0.1.0` world. The block between
the SDK-IMPORTS markers is the import set a real Python SDK pulls in at module level
(pre-init); `run.sh` strips it to build the `py-base` baseline from the same file.

Workflows:
- `hello`: one step returning a greeting, then completes (fuel/instantiate reference).
- `classes`: reports which stdlib modules ended up in `sys.modules` (snapshot evidence).
- `loop-check`: runs `loop_proto.selfcheck()` on the embedded CPython (only in the
  `py-loop` build, where run.sh stages loop_proto.py next to this file). It must be
  imported at module level: with `--stub-wasi` there is no filesystem at run time, so a
  first import inside `run` traps (see `lazy-import`).
- `lazy-import`: imports a stdlib module (`fractions`) for the first time inside `run`.
"""

# --- SDK-IMPORTS BEGIN ---
import asyncio
import contextvars
import dataclasses
import datetime
import json
import random
import uuid

# Touch each module so the snapshot carries initialised module state, as an SDK would.
_SDK_TOUCH = (
    asyncio.AbstractEventLoop,
    contextvars.ContextVar,
    dataclasses.dataclass,
    datetime.datetime,
    json.dumps,
    random.Random,
    uuid.UUID,
)
# --- SDK-IMPORTS END ---

import sys

try:  # staged only in the py-loop build
    import loop_proto
except ImportError:
    loop_proto = None

import wit_world
from componentize_py_types import Ok
from wit_world.exports import workflows
from wit_world.imports import journal, types

_JSON = "application/json"


def _payload(text: str) -> types.Payload:
    return types.Payload(_JSON, text.encode("utf-8"))


class Workflows(wit_world.exports.Workflows):
    def describe(self) -> list[workflows.WorkflowMeta]:
        return [
            workflows.WorkflowMeta("hello", 1, None, None),
            workflows.WorkflowMeta("classes", 1, None, None),
            workflows.WorkflowMeta("loop-check", 1, None, None),
            workflows.WorkflowMeta("lazy-import", 1, None, None),
        ]

    def run(self, name: str, input: types.Payload) -> workflows.RunResult:
        if name == "classes":
            mods = sorted(
                m
                for m in ("asyncio", "contextvars", "dataclasses", "datetime", "json", "random", "uuid")
                if m in sys.modules
            )
            return workflows.RunResult_Completed(_payload('{"modules":%s}' % _list(mods)))
        if name == "loop-check":
            if loop_proto is None:
                return workflows.RunResult_Completed(_payload('{"error":"loop_proto not staged"}'))
            return workflows.RunResult_Completed(_payload(json.dumps(loop_proto.selfcheck())))
        if name == "lazy-import":
            import fractions

            return workflows.RunResult_Completed(_payload('"%s"' % fractions.Fraction(1, 3)))
        start = journal.begin_step("greet", types.StepOptions(None, None, None))
        if isinstance(start, journal.StepStart_Replayed):
            out = start.value.value
        else:
            out = _payload('"hello, ostro"')
            with start.value as step:
                step.complete(Ok(out))
        return workflows.RunResult_Completed(out)


def _list(items: list[str]) -> str:
    return "[" + ",".join('"%s"' % i for i in items) + "]"
