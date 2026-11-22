"""Fixtures for the ostro-client tests.

- ``stub``: a local ``http.server`` in a thread that records every request and answers
  with queued responses (unit tests).
- ``ostro_server``: a spawned ``ostro dev --ephemeral --time-skipping=manual`` from
  ``OSTRO_BIN`` (else ``<repo>/target/debug/ostro``), addressed through its frozen ready line
  (D-P10-9), killed at teardown (e2e).
- ``p10_time_wasm``: the ``guests/p10-time`` component (``OSTRO_E2E_WASM``, else built with
  cargo into ``<repo>/target/guests``).
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

import pytest

HERE = pathlib.Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
REPO = HERE.parents[3]

#: The frozen ready line (D-P10-9), printed on stdout.
READY = re.compile(r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$")


# --- unit-test stub -------------------------------------------------------------------


class Stub:
    """Records requests; answers each with the next queued ``(status, body)`` (default 200 {})."""

    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []
        self.responses: List[Tuple[int, Any]] = []
        self.delay = 0.0
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a: Any) -> None:
                pass

            def _handle(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                with stub.lock:
                    stub.requests.append(
                        {
                            "method": self.command,
                            "path": self.path,
                            "headers": dict(self.headers),
                            "body": raw,
                        }
                    )
                    status, body = stub.responses.pop(0) if stub.responses else (200, {})
                if stub.delay:
                    import time

                    time.sleep(stub.delay)
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # the client gave up (timeout test)

            do_GET = do_POST = do_PUT = do_DELETE = _handle

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def reply(self, status: int, body: Any) -> None:
        self.responses.append((status, body))

    @property
    def last(self) -> Dict[str, Any]:
        return self.requests[-1]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub():
    s = Stub()
    yield s
    s.close()


# --- spawned server -------------------------------------------------------------------


def ostro_bin() -> pathlib.Path:
    p = os.environ.get("OSTRO_BIN")
    path = pathlib.Path(p) if p else REPO / "target" / "debug" / "ostro"
    if not path.is_file():
        pytest.fail(
            f"the ostro binary {path} does not exist: build it with `cargo build -p ostro` "
            "and/or point OSTRO_BIN at it (scripts/ci-local.sh sets OSTRO_BIN)",
            pytrace=False,
        )
    return path


class Server:
    def __init__(self, proc: subprocess.Popen, url: str, data_dir: str, ephemeral: bool, stderr: List[str]):
        self.proc = proc
        self.url = url
        self.data_dir = data_dir
        self.ephemeral = ephemeral
        self._stderr = stderr

    @property
    def stderr(self) -> str:
        return "".join(self._stderr)


def spawn_server(extra: List[str], cwd: str) -> Server:
    env = {k: v for k, v in os.environ.items() if not k.startswith("OSTRO_") and k != "RUST_LOG"}
    proc = subprocess.Popen(
        [str(ostro_bin()), "dev", "--ephemeral", "--listen", "127.0.0.1:0", *extra],
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ready: Dict[str, Any] = {}
    got = threading.Event()
    err: List[str] = []

    def read_out() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            m = READY.match(line.rstrip("\n"))
            if m and not got.is_set():
                ready["m"] = m
                got.set()

    def read_err() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            err.append(line)

    threading.Thread(target=read_out, daemon=True).start()
    threading.Thread(target=read_err, daemon=True).start()
    if not got.wait(60):
        proc.kill()
        proc.wait()
        pytest.fail(f"no ready line from ostro dev within 60 s; stderr: {''.join(err)}", pytrace=False)
    m = ready["m"]
    return Server(proc, f"http://{m.group(2)}", m.group(3), m.group(4) is not None, err)


@pytest.fixture
def ostro_server():
    with tempfile.TemporaryDirectory(prefix="ostro-client-py-") as cwd:
        s = spawn_server(["--time-skipping=manual"], cwd)
        try:
            yield s
        finally:
            s.proc.kill()
            s.proc.wait()


# --- guest ------------------------------------------------------------------------------


def _guest_wasm(name: str) -> bytes:
    p = os.environ.get("OSTRO_E2E_WASM")
    if p:
        return pathlib.Path(p).read_bytes()
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
    return (target / "wasm32-wasip2" / "release" / f"{name.replace('-', '_')}.wasm").read_bytes()


@pytest.fixture(scope="session")
def p10_time_wasm() -> bytes:
    return _guest_wasm("p10-time")


def unknown_id(run_id: str) -> str:
    """A well-formed run id that does not exist (one character of a real one changed)."""
    c = list(run_id)
    c[10] = "1" if c[10] == "0" else "0"
    return "".join(c)


def kinds(history: Dict[str, Any]) -> List[str]:
    return [str(e.get("kind")) for e in history.get("events", [])]


def find_kind(history: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
    for e in history.get("events", []):
        if e.get("kind") == kind:
            return e
    return None
