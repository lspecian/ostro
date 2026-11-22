"""`ostro replay` fixture v2 (plan 10-11, CLI-07 SC3), mirroring guests/p10-replay-v2.

v2: steps swapped with no patch, deliberately non-deterministic on a v1 journal.
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


@ostro.workflow(name="py_replay_demo", version=2)
async def replay_demo(ctx: ostro.Ctx, input: object) -> list:
    steps = [await b(), await a()]
    await ctx.sleep(sleep_of(input))
    return steps


ostro.export()
