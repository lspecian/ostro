"""Raw-binding Python smoke fixture (plan 09-02): no SDK, generated componentize-py
0.25.1 bindings only.

Workflow `hello` v1: step `greet` records `{"hello": <input>}` and the run completes with
that payload. componentize-py discovers the export implementation by the interface name:
the entry module must define a class `Workflows` implementing
`wit_world.exports.Workflows` (world export `workflows`).
"""
import json

from componentize_py_types import Err, Ok
from wit_world import exports
from wit_world.exports import workflows
from wit_world.imports import journal, types

JSON = "application/json"


def _decode(p: types.Payload):
    return json.loads(p.data) if p.data else None


def _encode(v) -> types.Payload:
    return types.Payload(content_type=JSON, data=json.dumps(v, separators=(",", ":")).encode())


class Workflows(exports.Workflows):
    def describe(self) -> list[workflows.WorkflowMeta]:
        return [workflows.WorkflowMeta(name="hello", version=1, default_queue=None, run_timeout_ms=None)]

    def run(self, name: str, input: types.Payload) -> workflows.RunResult:
        if name != "hello":
            raise Err(types.Failure_NonRetryable(types.ErrorInfo(kind="unknown_workflow", message=name, details=None)))
        opts = types.StepOptions(retry=None, timeout_ms=None, fuel=None)
        start = journal.begin_step("greet", opts)
        if isinstance(start, journal.StepStart_Replayed):
            outcome = start.value
            if isinstance(outcome, Err):
                raise outcome
            return workflows.RunResult_Completed(outcome.value)
        out = _encode({"hello": _decode(input)})
        with start.value as step:
            step.complete(Ok(out))
        return workflows.RunResult_Completed(out)
