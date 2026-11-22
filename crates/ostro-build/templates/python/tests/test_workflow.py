"""Runs ``hello`` against a throwaway ``ostro dev --ephemeral --time-skipping`` server:
the three-day sleep is skipped automatically, so the test finishes in seconds.

Needs the ``ostro`` binary (``OSTRO_BIN`` or ``PATH``). Run with ``pytest``.
"""

THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000


def test_hello_sleeps_three_days(ostro_env, ostro_project):
    # ostro_project has built and deployed this project once for the session.
    run = ostro_env.start_run("hello", {"name": "ada"})
    ostro_env.signal(run, "approve", {"ok": True})
    res = ostro_env.result(run, wait="30s")  # auto time-skip covers the 3-day sleep
    assert res["status"] == "completed"
    assert res["output"] == {"greeting": "hello ada", "approved": True}
    assert ostro_env.now_ms() - run.accepted_at_ms >= THREE_DAYS_MS
