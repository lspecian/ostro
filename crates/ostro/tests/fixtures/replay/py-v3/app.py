"""`ostro replay` fixture v3 (plan 10-11, CLI-07 SC3), mirroring guests/p10-replay-v3.

v3: the swap behind `ctx.patched("reorder")`; v1 journals take the old order.
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


@ostro.workflow(name="py_replay_demo", version=3)
async def replay_demo(ctx: ostro.Ctx, input: object) -> list:
    if ctx.patched("reorder"):
        steps = [await b(), await a()]
    else:
        steps = [await a(), await b()]
    await ctx.sleep(sleep_of(input))
    return steps


ostro.export()
