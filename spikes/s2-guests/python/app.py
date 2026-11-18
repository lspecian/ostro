"""Spike S2 Python guest (componentize-py 0.25.1, --stub-wasi). Throwaway."""
import json
import random
import time
import uuid

import wit_world
from wit_world.imports import context, journal


class WitWorld(wit_world.WitWorld):
    def run(self, input: str) -> str:
        start = journal.begin_step("greet")
        if isinstance(start, journal.StepStart_Replayed):
            return start.value
        out = "hello " + input
        with start.value as step:
            res_seq = step.seq()
            step.complete(out)
        h = journal.begin_step_u64("greet-u64")
        journal.complete_u64(h, out)
        return json.dumps({"out": out, "resource_seq": res_seq, "u64_seq": h})

    def probe_random(self) -> str:
        return json.dumps([random.random() for _ in range(3)])

    def probe_time(self) -> str:
        return json.dumps(time.time())

    def probe_hash(self) -> str:
        return json.dumps([hash("ostro"), list({"a", "b", "c", "d"})])

    def probe_uuid(self) -> str:
        return str(uuid.uuid4())

    def run_reseeded(self, seed_from_host: bool) -> str:
        seed = None
        if seed_from_host:
            seed = context.random_u64()
            random.seed(seed)
        return json.dumps({"seed": seed, "draws": [random.random() for _ in range(3)]})

    def run_dropped_step(self) -> str:
        start = journal.begin_step("dropped")
        if isinstance(start, journal.StepStart_Replayed):
            return json.dumps({"replayed": start.value})
        step = start.value
        seq = step.seq()
        # Deliberately no complete(); release the handle explicitly so the
        # host's HostStep::drop runs before the export returns.
        step.__exit__(None, None, None)
        return json.dumps({"dropped_seq": seq})
