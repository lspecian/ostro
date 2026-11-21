"""Hello, Ostro: the smallest Python workflow module built with the SDK.

Build it with ``sdks/scripts/build-guest.sh py sdks/python/examples/hello hello.wasm``.
The component imports only ``ostro:runtime/*@0.1.0`` and exports ``workflows`` (plus
componentize-py's ``exports`` init hook).
"""

import asyncio

import ostro


@ostro.step
def greet(name: str) -> str:
    return f"hello, {name}"


@ostro.workflow(name="hello")
async def hello(ctx: ostro.Ctx, name: str) -> dict:
    greeting = await greet(name)
    # Two durable timers in parallel: one wait, both complete after 2 s.
    await asyncio.gather(ctx.sleep(1), ctx.sleep(2))
    return {"greeting": greeting, "lucky": ctx.random_f64()}


ostro.export()
