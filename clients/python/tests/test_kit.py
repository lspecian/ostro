"""The Python test kit (``ostro_client.testing``) and its pytest plugin against a spawned
``ostro dev --ephemeral --time-skipping`` (10-17)."""

from __future__ import annotations

import base64
import os
import pathlib
import subprocess
import sys
import textwrap
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from conftest import REPO, ostro_bin
from ostro_client.testing import READY_LINE, READY_LINE_PATTERN, TestEnv, TestEnvError, ostro_bin as kit_bin

DAY_MS = 24 * 60 * 60 * 1000
CLIENT_DIR = pathlib.Path(__file__).resolve().parents[1]


def build_guest(name: str) -> pathlib.Path:
    """``guests/<name>`` built with cargo into ``<repo>/target/guests`` (its path)."""
    target = REPO / "target" / "guests"
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR", "RUSTC_WRAPPER")
    }
    r = subprocess.run(
        [
            os.environ.get("CARGO", "cargo"),
            "build",
            "--manifest-path",
            str(REPO / "guests" / "Cargo.toml"),
            "-p",
            name,
            "--target",
            "wasm32-wasip2",
            "--release",
            "--target-dir",
            str(target),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        pytest.fail(f"building guests/{name} failed:\n{r.stderr}", pytrace=False)
    return target / "wasm32-wasip2" / "release" / f"{name.replace('-', '_')}.wasm"


@pytest.fixture(scope="module")
def p10_time_path() -> pathlib.Path:
    p = os.environ.get("OSTRO_E2E_WASM")
    return pathlib.Path(p) if p else build_guest("p10-time")


@pytest.fixture(scope="module")
def env(p10_time_path: pathlib.Path):
    with TestEnv.start(binary=str(ostro_bin())) as e:
        e.deploy_wasm(p10_time_path, name="p10-time")
        yield e


def test_ready_line_pattern_is_frozen():
    assert READY_LINE_PATTERN == r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$"
    m = READY_LINE.match("ostro dev listening on http://127.0.0.1:1 (data: /tmp/a b, ephemeral) ready in 5 ms")
    assert m and m.group(2) == "127.0.0.1:1" and m.group(3) == "/tmp/a b" and m.group(4)


def test_three_day_sleep_completes_in_seconds(env: TestEnv):
    t0 = time.monotonic()
    run = env.start_run("sleep_for", {"ms": 3 * DAY_MS})
    res = env.result(run, wait="30s")
    wall = time.monotonic() - t0
    assert res["status"] == "completed", res
    assert res["output"] == {"slept": 3 * DAY_MS}
    assert wall < 10, wall
    assert env.now_ms() - run.accepted_at_ms >= 3 * DAY_MS
    timers = env.timers(run)
    assert len(timers) == 1 and timers[0]["fired"], timers
    assert timers[0]["fire_at_ms"] == run.accepted_at_ms + 3 * DAY_MS
    kinds = [e["kind"] for e in env.history(run)]
    assert kinds[0] == "run_started" and kinds[-1] == "run_completed", kinds
    assert env.assert_completed(run, {"slept": 3 * DAY_MS}) == {"slept": 3 * DAY_MS}
    print(f"kit wall time: 3-day sleep_for {wall:.3f} s")


def test_signal_then_sleep_returns_the_payload(env: TestEnv):
    run = env.start_run("signal_then_sleep", {})
    env.signal(run, "go", {"ok": 7})
    assert env.assert_completed(run.run_id) == {"ok": 7}
    assert env.now_ms() - run.accepted_at_ms >= DAY_MS
    assert env.steps(run) == []


def test_skip_advances_the_clock(env: TestEnv):
    before = env.now_ms()
    s = env.skip("72h")
    assert s["skipped_ms"] >= 72 * 3_600_000
    assert env.now_ms() - before >= 72 * 3_600_000


class _Mock(BaseHTTPRequestHandler):
    def log_message(self, *a: Any) -> None:
        pass

    def do_GET(self) -> None:
        body = b"hello from the mock" if self.path == "/hello" else b"nope"
        self.send_response(200 if self.path == "/hello" else 404)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def test_loopback_http_mock_with_allow_egress():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Mock)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        with TestEnv.start(binary=str(ostro_bin()), egress_loopback=True) as e:
            e.deploy_wasm(build_guest("fx-http"))  # module "fx-http" (file stem)
            e.allow_egress("fx-http", origin)
            run = e.start_run("fx-http", {"requests": [{"method": "GET", "url": f"{origin}/hello"}], "parallel": False})
            out = e.assert_completed(run)
            r = out["results"][0]
            assert r["status"] == 200, out
            assert base64.b64decode(r["body_b64"]) == b"hello from the mock"
    finally:
        srv.shutdown()
        srv.server_close()


def test_close_kills_the_server():
    e = TestEnv.start(binary=str(ostro_bin()))
    pid = e.pid
    e.close()
    e.close()  # idempotent
    deadline = time.monotonic() + 2
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        assert time.monotonic() < deadline, f"pid {pid} still alive"
        time.sleep(0.02)


def test_missing_ostro_bin_is_a_clear_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OSTRO_BIN", "/nonexistent/ostro-kit-test")
    with pytest.raises(TestEnvError, match="OSTRO_BIN=/nonexistent/ostro-kit-test"):
        kit_bin()
    with pytest.raises(TestEnvError, match="OSTRO_BIN"):
        TestEnv.start()


def test_pytest_plugin_fixtures(tmp_path: pathlib.Path, p10_time_path: pathlib.Path):
    """A project using the plugin's fixtures, run in a subprocess ``python -m pytest``."""
    (tmp_path / "conftest.py").write_text(
        textwrap.dedent(
            f"""
            import sys
            sys.path.insert(0, {str(CLIENT_DIR)!r})
            pytest_plugins = ["ostro_client.pytest_plugin"]
            """
        )
    )
    (tmp_path / "test_project.py").write_text(
        textwrap.dedent(
            """
            def test_env(ostro_env):
                assert ostro_env.now_ms() > 0

            def test_project(ostro_project, ostro_env):
                assert any(w["name"] == "sleep_for" for w in ostro_project["module"]["workflows"])
                run = ostro_env.start_run("sleep_for", {"ms": 3 * 86400000})
                assert ostro_env.assert_completed(run) == {"slept": 3 * 86400000}
            """
        )
    )
    env = dict(os.environ, OSTRO_BIN=str(ostro_bin()), OSTRO_PROJECT_WASM=str(p10_time_path))
    r = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(tmp_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert "2 passed" in r.stdout, r.stdout
