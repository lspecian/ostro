"""pytest setup for the Ostro test kit (``ostro_client``).

Until ``ostro-client`` is installed from PyPI, the kit is imported from the SDK bundle
that ``ostro init`` / ``ostro build`` materialise under ``target/ostro/sdk/clients/python``.
An installed ``ostro-client`` registers its pytest plugin itself (entry point ``ostro``).
"""

import pathlib
import sys
from importlib import metadata

try:
    metadata.distribution("ostro-client")
except metadata.PackageNotFoundError:
    _root = pathlib.Path(__file__).resolve().parents[1]
    _bundled = _root.joinpath("target", "ostro", "sdk", "clients", "python")
    try:
        import ostro_client  # noqa: F401
    except ImportError:
        sys.path.insert(0, str(_bundled))
    pytest_plugins = ["ostro_client.pytest_plugin"]
