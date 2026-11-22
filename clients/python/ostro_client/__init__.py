"""Python client for the Ostro REST API (SDK-07).

Standard library only: no runtime dependencies. The route table (``routes.py``) is checked
against the server's OpenAPI document by ``crates/ostro-client/tests/contract.rs``.

    from ostro_client import Client

    c = Client("http://127.0.0.1:7700")
    run = c.start_run("order", {"id": 7})
    print(c.result(run["run_id"], wait="30s"))
"""

from .client import DEFAULT_SERVER, Client, parse_duration, sha256_hex
from .errors import OstroError
from .routes import ROUTES

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_SERVER",
    "ROUTES",
    "Client",
    "OstroError",
    "__version__",
    "parse_duration",
    "sha256_hex",
]
