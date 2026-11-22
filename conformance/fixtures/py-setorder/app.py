"""Set-iteration-order fixture for C-REBUILD-01 (plan 09-17, D-P9-3).

``NAMES`` is a ``set`` of 20 strings built at import, i.e. during componentize-py's
pre-init, so its iteration order follows the str hash secret baked into the snapshot.
``setorder`` issues one step per name in that order: two builds of this same source
journal the same step sequence only when the hash secret is pinned at pre-init
(PYTHONHASHSEED=0, the D-P9-3 fork). With the released componentize-py 0.25.1 every
build bakes a fresh secret, the order changes, and replaying one build's journal on
another build must be detected as non-determinism (never silently accepted).

Lesson for workflow code (docs/sdk-builds.md): never feed an unsorted set of str into
commands; iterate ``sorted(...)`` instead.
"""

import ostro

NAMES = {f"item-{i:02d}" for i in range(20)}


@ostro.workflow(name="setorder")
async def setorder(ctx: ostro.Ctx, input: object) -> dict:
    order = []
    for name in NAMES:
        await ostro.step(name=name)(lambda n=name: n)()
        order.append(name)
    return {"order": order, "hash": hash("ostro")}


ostro.export()
