"""pytest plugin for the Ostro test kit (entry point ``pytest11: ostro``).

Fixtures:

- ``ostro_env`` (session): a :class:`ostro_client.testing.TestEnv`, a throwaway
  ``ostro dev --ephemeral --time-skipping`` server, closed at the end of the session.
- ``ostro_project`` (session): deploys the project once per session and returns the
  ``ostro deploy --json`` result. The project is ``$OSTRO_PROJECT_DIR``, else pytest's
  rootdir. ``$OSTRO_PROJECT_WASM`` deploys that prebuilt component instead of building.

Options (environment): ``OSTRO_BIN`` (the binary), ``OSTRO_TEST_EGRESS_LOOPBACK=1``
(``ostro_env`` lets effects reach 127.0.0.0/8, for HTTP mocks).

An installed ``ostro-client`` registers this plugin itself. Without installation, add
``pytest_plugins = ["ostro_client.pytest_plugin"]`` to ``conftest.py``. The module is
only loaded by pytest, so importing ``ostro_client`` never imports pytest.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterator

import pytest

from .testing import TestEnv, TestEnvError


@pytest.fixture(scope="session")
def ostro_env() -> Iterator[TestEnv]:
    """A spawned ``ostro dev --ephemeral --time-skipping`` for the whole session."""
    loopback = os.environ.get("OSTRO_TEST_EGRESS_LOOPBACK", "") not in ("", "0", "false")
    try:
        env = TestEnv.start(egress_loopback=loopback)
    except TestEnvError as e:
        pytest.fail(str(e), pytrace=False)
    try:
        yield env
    finally:
        env.close()


@pytest.fixture(scope="session")
def ostro_project(ostro_env: TestEnv, request: Any) -> Dict[str, Any]:
    """Deploy the project (``$OSTRO_PROJECT_DIR`` or the rootdir) once per session."""
    wasm = os.environ.get("OSTRO_PROJECT_WASM")
    try:
        if wasm:
            return ostro_env.deploy_wasm(wasm)
        project = os.environ.get("OSTRO_PROJECT_DIR") or str(request.config.rootpath)
        return ostro_env.build_and_deploy(project)
    except TestEnvError as e:
        pytest.fail(str(e), pytrace=False)
