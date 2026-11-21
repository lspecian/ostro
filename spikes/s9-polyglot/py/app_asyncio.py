"""Spike S9 Python guest (componentize-py 0.25.1, --stub-wasi). Throwaway.

Raw bindings against the real `ostro:runtime/workflow@0.1.0` world. The block between
the SDK-IMPORTS markers is the import set a real Python SDK pulls in at module level
(pre-init); `run.sh` strips it to build the `py-base` baseline from the same file.

Workflows:
- `hello`: one step returning a greeting, then completes (fuel/instantiate reference).
- `classes`: reports which stdlib modules ended up in `sys.modules` (snapshot evidence).
- `loop-check`: runs `loop_proto.selfcheck()` on the embedded CPython (only in the
  `py-loop` build, where run.sh stages loop_proto.py next to this file).
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
            import json as _json

            try:
                import loop_proto
            except ImportError as e:
                return workflows.RunResult_Completed(_payload(_json.dumps({"error": str(e)})))
            return workflows.RunResult_Completed(_payload(_json.dumps(loop_proto.selfcheck())))
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
