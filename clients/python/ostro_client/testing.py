"""The spawned-server test kit (SDK-06): ``TestEnv`` starts a throwaway
``ostro dev --ephemeral --time-skipping`` and drives it through :class:`ostro_client.Client`.

The server skips its dev clock forward whenever it is idle and a result long-poll is
waiting, so ``env.result(run, wait="30s")`` on a workflow that sleeps for days returns in
milliseconds of wall time.

    from ostro_client.testing import TestEnv

    with TestEnv.start() as env:
        env.build_and_deploy(".")
        run = env.start_run("hello", {"name": "ada"})
        env.signal(run, "approve", {"ok": True})
        assert env.result(run, wait="30s")["status"] == "completed"

With pytest, the plugin ``ostro_client.pytest_plugin`` (entry point ``ostro``) provides the
``ostro_env`` and ``ostro_project`` fixtures.

Standard library only. The binary is ``OSTRO_BIN``, else ``ostro`` on ``PATH``; its
``ostro build-info --json`` version must be at least this package's version.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Mapping, Optional, Union

from .client import Client
from .errors import OstroError

__all__ = [
    "READY_LINE",
    "READY_LINE_PATTERN",
    "RETENTION",
    "Run",
    "TestEnv",
    "TestEnvError",
    "ostro_bin",
]

#: The frozen ready line (D-P10-9, amended); the same pattern in every kit.
READY_LINE_PATTERN = r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$"
READY_LINE = re.compile(READY_LINE_PATTERN)

#: Run retention given to the server: skipped days must not expire runs under test.
RETENTION = "87600h"

READY_TIMEOUT_S = 30.0
KIT_VERSION = "0.1.0"

Json = Any


class TestEnvError(Exception):
    """The binary is missing or too old, the server did not start, or a deploy failed."""

    __test__ = False  # not a pytest test class


@dataclass(frozen=True)
class Run:
    """A started run. ``accepted_at_ms`` is server (dev clock) time."""

    run_id: str
    workflow: str
    accepted_at_ms: int

    def __str__(self) -> str:
        return self.run_id


RunRef = Union[Run, str]


def _rid(run: RunRef) -> str:
    return run.run_id if isinstance(run, Run) else str(run)


def ostro_bin(binary: Optional[str] = None) -> str:
    """``binary``, else ``$OSTRO_BIN``, else ``ostro`` on ``PATH``."""
    if binary:
        if not os.path.isfile(binary):
            raise TestEnvError(f"the ostro binary {binary} does not exist")
        return binary
    env = os.environ.get("OSTRO_BIN")
    if env:
        if not os.path.isfile(env):
            raise TestEnvError(
                f"OSTRO_BIN={env} does not exist: point OSTRO_BIN at a built ostro binary "
                "(cargo build -p ostro) or unset it to use `ostro` on PATH"
            )
        return env
    found = shutil.which("ostro")
    if not found:
        raise TestEnvError("no `ostro` on PATH and OSTRO_BIN is not set: install ostro or point OSTRO_BIN at the binary")
    return found


def _triple(v: str) -> Optional[tuple]:
    core = re.split(r"[-+]", v, maxsplit=1)[0].split(".")
    try:
        parts = [int(p) for p in core]
    except ValueError:
        return None
    if len(parts) < 2:
        return None
    return tuple((parts + [0, 0, 0])[:3])


def check_version(binary: str) -> str:
    """``ostro build-info --json`` -> its version; :class:`TestEnvError` when older than the kit."""
    try:
        out = subprocess.run([binary, "build-info", "--json"], capture_output=True, text=True, timeout=30)
    except OSError as e:
        raise TestEnvError(f"cannot run {binary}: {e}") from None
    if out.returncode != 0:
        raise TestEnvError(f"`{binary} build-info --json` failed: {out.stderr.strip()}")
    try:
        version = str(json.loads(out.stdout).get("version", ""))
    except ValueError:
        raise TestEnvError(f"`{binary} build-info --json` printed no JSON") from None
    have, need = _triple(version), _triple(KIT_VERSION)
    if have is None or need is None or have < need:
        raise TestEnvError(f"{binary} is version {version!r}; this test kit needs ostro >= {KIT_VERSION}")
    return version


def _server_env(extra: Optional[Mapping[str, str]], egress_loopback: bool) -> Dict[str, str]:
    # Inherited server configuration (OSTRO_<SECTION>__*) and RUST_LOG are dropped.
    env = {
        k: v
        for k, v in os.environ.items()
        if not (k == "RUST_LOG" or k == "OSTRO_LISTEN" or (k.startswith("OSTRO_") and "__" in k))
    }
    env["OSTRO_RUNS__RETENTION"] = RETENTION
    if egress_loopback:
        env["OSTRO_EGRESS__ALLOW_PRIVATE"] = "127.0.0.0/8"
    env.update(extra or {})
    return env


class TestEnv:
    """A throwaway ``ostro dev --ephemeral --time-skipping`` server. Use :meth:`start`."""

    __test__ = False  # not a pytest test class

    def __init__(self, proc: subprocess.Popen, match: "re.Match[str]", binary: str, cwd: str, stderr: Deque[str], timeout: float):
        self._proc: Optional[subprocess.Popen] = proc
        self.pid: int = proc.pid
        self.binary = binary
        self.addr: str = match.group(2)
        self.url = f"http://{self.addr}"
        self.data_dir: str = match.group(3)
        self.ephemeral: bool = match.group(4) is not None
        self.ready_ms = int(match.group(5))
        self.client = Client(self.url, timeout=timeout)
        self._cwd = cwd
        self._stderr = stderr
        self._modules: Dict[str, str] = {}

    # --- lifecycle ------------------------------------------------------------------

    @classmethod
    def start(
        cls,
        binary: Optional[str] = None,
        egress_loopback: bool = False,
        env: Optional[Mapping[str, str]] = None,
        time_skipping: str = "auto",
        timeout: float = 30.0,
    ) -> "TestEnv":
        """Spawn the server and wait (30 s) for its ready line.

        ``egress_loopback`` lets effects reach ``127.0.0.0/8`` (HTTP mocks; a module still
        needs :meth:`allow_egress`). ``env`` adds server environment variables.
        ``time_skipping="manual"`` skips only through :meth:`skip`.
        """
        exe = ostro_bin(binary)
        check_version(exe)
        cwd = tempfile.mkdtemp(prefix="ostro-testenv-")
        try:
            proc = subprocess.Popen(
                [exe, "dev", "--ephemeral", "--listen", "127.0.0.1:0", f"--time-skipping={time_skipping}"],
                cwd=cwd,
                env=_server_env(env, egress_loopback),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as e:
            shutil.rmtree(cwd, ignore_errors=True)
            raise TestEnvError(f"cannot spawn {exe} dev: {e}") from None
        got = threading.Event()
        found: Dict[str, "re.Match[str]"] = {}
        err: Deque[str] = deque(maxlen=2000)

        def read_out() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:  # drained to the end: the server logs on stdout
                if not got.is_set():
                    m = READY_LINE.match(line.rstrip("\r\n"))
                    if m:
                        found["m"] = m
                        got.set()
            got.set()

        def read_err() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                err.append(line)

        threading.Thread(target=read_out, daemon=True).start()
        threading.Thread(target=read_err, daemon=True).start()
        got.wait(READY_TIMEOUT_S)
        if "m" not in found:
            why = "exited before printing the ready line" if proc.poll() is not None else "printed no ready line within 30 s"
            proc.kill()
            proc.wait()
            time.sleep(0.05)
            shutil.rmtree(cwd, ignore_errors=True)
            raise TestEnvError(f"ostro dev {why} ({exe}); stderr:\n{''.join(err)[-4096:]}")
        return cls(proc, found["m"], exe, cwd, err, timeout)

    def close(self) -> None:
        """Kill and reap the server and remove its temp directory (idempotent)."""
        proc, self._proc = self._proc, None
        if proc is not None:
            proc.kill()
            proc.wait()
            for s in (proc.stdout, proc.stderr):
                try:
                    if s is not None:
                        s.close()
                except OSError:
                    pass
        shutil.rmtree(self._cwd, ignore_errors=True)

    def __enter__(self) -> "TestEnv":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @property
    def stderr(self) -> str:
        """The tail of the server's stderr."""
        return "".join(self._stderr)

    # --- deploy ---------------------------------------------------------------------

    def _remember(self, module: str, workflows: List[str]) -> None:
        if module:
            for w in workflows:
                self._modules[w] = module
            self._modules[module] = module

    def build_and_deploy(self, project_dir: Union[str, os.PathLike] = ".") -> Dict[str, Any]:
        """``ostro deploy <dir> --activate --json --server <url>``: build the project if stale,
        upload it (module name = the project name) and activate its new versions.

        The server refuses a changed module under an existing workflow name and version:
        bump the version in the source to redeploy a change into the same server.
        """
        r = subprocess.run(
            [self.binary, "deploy", str(project_dir), "--activate", "--json", "--server", self.url],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            raise TestEnvError(f"`ostro deploy {project_dir}` failed (exit {r.returncode}):\n{r.stderr}{r.stdout}")
        out = json.loads(r.stdout)
        build = out.get("build") or {}
        self._remember(str(build.get("name") or ""), [w["name"] for w in build.get("workflows") or []])
        return out

    def deploy_wasm(self, path: Union[str, os.PathLike], name: Optional[str] = None) -> Dict[str, Any]:
        """Upload a prebuilt component as module ``name`` (default: the file stem with ``_``
        -> ``-``) and activate every workflow version it declares."""
        p = pathlib.Path(path)
        return self.deploy_wasm_bytes(p.read_bytes(), name or p.stem.replace("_", "-"))

    def deploy_wasm_bytes(self, wasm: bytes, name: str) -> Dict[str, Any]:
        up = self.client.upload_module(wasm, name=name)
        wfs = up.get("module", {}).get("workflows", [])
        for w in wfs:
            self.client.activate(w["name"], int(w["version"]))
        self._remember(name, [w["name"] for w in wfs])
        return up

    # --- runs -----------------------------------------------------------------------

    def start_run(self, workflow: str, input: Json = None, **opts: Any) -> Run:
        """Start a run (``opts``: the :meth:`Client.start_run` keywords)."""
        started = self.client.start_run(workflow, input, **opts)
        info = self.client.get_run(started["run_id"])
        return Run(started["run_id"], str(info.get("workflow", workflow)), int(info.get("created_at_ms") or 0))

    def signal(self, run: RunRef, name: str, payload: Json = None) -> Dict[str, Any]:
        return self.client.signal(_rid(run), name, payload)

    def result(self, run: RunRef, wait: Union[str, float] = "30s") -> Dict[str, Any]:
        """The run's result (``{run_id, completed, status, result, failure}``) plus ``output``
        (the workflow output, the same value as the wire field ``result``), waiting up to
        ``wait``; the wait lets the server skip idle time."""
        r = dict(self.client.result(_rid(run), wait=wait))
        r["output"] = r.get("result")
        return r

    def skip(self, by: Union[str, float]) -> Dict[str, Any]:
        """Skip the dev clock forward (``"72h"``). 503 ``busy`` while activations are in flight."""
        return self.client.dev_skip(by)

    def now_ms(self) -> int:
        """Server (dev clock) time in ms since the epoch."""
        return int(self.client.dev_time()["now_ms"])

    def history(self, run: RunRef) -> List[Dict[str, Any]]:
        """Every history event (all pages): ``{seq, kind, ts_ms, summary, details}``."""
        events: List[Dict[str, Any]] = []
        frm: Optional[int] = None
        while True:
            page = self.client.history(_rid(run), from_seq=frm)
            events.extend(page.get("events") or [])
            nxt = page.get("next_from_seq")
            if nxt is None or nxt == frm:
                return events
            frm = int(nxt)

    def steps(self, run: RunRef) -> List[str]:
        """Names of the completed steps (``step_completed`` with an ``ok`` outcome)."""
        return [
            e["details"]["name"]
            for e in self.history(run)
            if e.get("kind") == "step_completed" and "ok" in (e.get("details", {}).get("outcome") or {})
        ]

    def timers(self, run: RunRef) -> List[Dict[str, Any]]:
        """Each ``timer_started`` with its ``timer_fired``:
        ``{seq, started_ms, fire_at_ms, fired, fired_seq, fired_ms}``."""
        events = self.history(run)
        timers = [
            {
                "seq": e["seq"],
                "started_ms": e.get("ts_ms"),
                "fire_at_ms": e.get("details", {}).get("fire_at_ms"),
                "fired": False,
                "fired_seq": None,
                "fired_ms": None,
            }
            for e in events
            if e.get("kind") == "timer_started"
        ]
        by_seq = {t["seq"]: t for t in timers}
        for e in events:
            if e.get("kind") == "timer_fired":
                t = by_seq.get(e.get("details", {}).get("op"))
                if t is not None:
                    t.update(fired=True, fired_seq=e["seq"], fired_ms=e.get("ts_ms"))
        return timers

    _MISSING: Any = object()

    def assert_completed(self, run: RunRef, output: Json = _MISSING, wait: Union[str, float] = "30s") -> Json:
        """Wait for ``run`` and assert it completed (with ``output`` when given); returns the output."""
        r = self.result(run, wait=wait)
        assert r.get("completed"), f"run {_rid(run)} did not finish within {wait} (status {r.get('status')})"
        assert r.get("status") == "completed", f"run {_rid(run)} ended {r.get('status')}: {r.get('failure')}"
        if output is not TestEnv._MISSING:
            assert r.get("output") == output, f"output of run {_rid(run)}: {r.get('output')!r} != {output!r}"
        return r.get("output")

    def allow_egress(self, module_or_workflow: str, url_prefix: str) -> Dict[str, Any]:
        """Allow a module (or a workflow deployed through this kit) to call ``url_prefix``
        (an egress rule such as ``http://127.0.0.1:8080``), keeping its existing rules.
        Loopback targets also need ``TestEnv.start(egress_loopback=True)``."""
        module = self._modules.get(module_or_workflow, module_or_workflow)
        c = self.client
        try:
            cur = c._request("GET", "/v1/egress/{module}", ["v1", "egress", module])
            allow = list((cur or {}).get("allow") or [])
        except OstroError as e:
            if e.status != 404:
                raise
            allow = []
        rule = url_prefix.rstrip("/")
        if rule not in allow:
            allow.append(rule)
        return c._request("PUT", "/v1/egress/{module}", ["v1", "egress", module], body={"allow": allow})
