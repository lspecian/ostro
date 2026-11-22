"""The shared client e2e scenario (SDK-07) against a spawned
``ostro dev --ephemeral --time-skipping=manual``."""

from __future__ import annotations

import time

import pytest

from conftest import find_kind, kinds, unknown_id
from ostro_client import Client, OstroError

DAY_MS = 24 * 3600 * 1000


def test_shared_scenario(ostro_server, p10_time_wasm):
    t0 = time.monotonic()
    assert ostro_server.ephemeral
    c = Client(ostro_server.url, timeout=30)

    up = c.upload_module(p10_time_wasm, name="p10-time")
    assert any(w["name"] == "signal_then_sleep" for w in up["module"]["workflows"]), up
    wf = c.activate("signal_then_sleep", 1)
    assert wf["active_version"] == 1, wf

    started = c.start_run("signal_then_sleep", {})
    run_id = started["run_id"]
    assert started["created"] is True
    # The run is live: `running`, or `scheduled`/`ready` between activations (a run parked
    # on its signal wait is reported either way).
    run = c.get_run(run_id)
    assert run["status"] in ("running", "scheduled", "ready"), run
    assert run["workflow"] == "signal_then_sleep", run

    sig = c.signal(run_id, "go", {"ok": True})
    assert sig["accepted"] is True, sig

    # Wait until the run is parked on its 1-day timer.
    deadline = time.monotonic() + 30
    while find_kind(c.history(run_id), "timer_started") is None:
        assert time.monotonic() < deadline, kinds(c.history(run_id))
        time.sleep(0.05)

    skip = c.dev_skip("24h")
    assert skip["skipped_ms"] >= DAY_MS, skip

    res = c.result(run_id, wait="30s")
    assert res["completed"] is True, res
    assert res["status"] == "completed", res
    assert res["result"] == {"ok": True}, res

    assert find_kind(c.history(run_id), "timer_fired") is not None, kinds(c.history(run_id))
    assert c.dev_time()["offset_ms"] >= DAY_MS

    with pytest.raises(OstroError) as ei:
        c.get_run(unknown_id(run_id))
    assert ei.value.status == 404, repr(ei.value)

    print(f"[wall] python e2e scenario: {time.monotonic() - t0:.2f}s")
