"""Ostro Python SDK (WIT ostro:runtime@0.1.0): durable workflows on asyncio.

```python
import ostro

@ostro.step
def greet(name: str) -> str:
    return f"hello, {name}"

@ostro.workflow(name="hello")
async def hello(ctx: ostro.Ctx, name: str) -> dict:
    return {"greeting": await greet(name)}

ostro.export()
```

Importing the package installs the determinism surface (`ostro._determinism`) and
imports everything the SDK uses at run time: inside a componentize-py guest a module
imported for the first time during ``run`` traps, so the import must happen at pre-init.
"""

from . import _determinism
from ._host import RunInfo
from .ctx import Ctx, f64_from_u64
from .errors import (
    Cancelled,
    Deadlock,
    InvalidString,
    NonRetryable,
    OstroError,
    ProtocolError,
    Retryable,
    Timeout,
)
from .export import WorkflowMeta, export, workflow
from .loop import OstroLoop
from .ops import ChildRun, Op
from .step import Retry, step

__version__ = "0.1.0"

__all__ = [
    "Cancelled",
    "ChildRun",
    "Ctx",
    "Deadlock",
    "InvalidString",
    "NonRetryable",
    "Op",
    "OstroError",
    "OstroLoop",
    "ProtocolError",
    "Retry",
    "Retryable",
    "RunInfo",
    "Timeout",
    "WorkflowMeta",
    "__version__",
    "export",
    "f64_from_u64",
    "step",
    "workflow",
]

_determinism.install()
