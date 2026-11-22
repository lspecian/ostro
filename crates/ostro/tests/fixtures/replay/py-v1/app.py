"""`ostro replay` fixture v1 (plan 10-11, CLI-07 SC3), mirroring guests/p10-replay-v1.

Workflow `py_replay_demo` v1: step `a`, step `b`, a durable sleep; returns the steps.
"""

import ostro


@ostro.step
def a() -> str:
    return "a"


@ostro.step
def b() -> str:
    return "b"


def sleep_of(input: object) -> int:
    """`input.sleep_s` (default 1 s)."""
    if isinstance(input, dict) and isinstance(input.get("sleep_s"), int):
        return input["sleep_s"]
    return 1


@ostro.workflow(name="py_replay_demo", version=1)
async def replay_demo(ctx: ostro.Ctx, input: object) -> list:
    steps = [await a(), await b()]
    await ctx.sleep(sleep_of(input))
    return steps


ostro.export()
