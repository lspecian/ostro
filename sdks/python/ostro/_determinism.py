"""The determinism surface (D-P9-15, WF-07), installed when ``ostro`` is imported.

`install()` runs at SDK import time, i.e. during componentize-py's pre-init snapshot, so
the patches are baked into the component. Imports trap during pre-init (PF-W8), so no
patch calls the host at import time: every patched function routes to the host only
while an activation is running (`ostro.loop.active()`), and otherwise falls back to the
original implementation (host-side tests, and pre-init itself).

Inside an activation:

- **Randomness → host** (so D7's step-local streams apply): ``random`` (the module-level
  functions are rebound to a `HostRandom` whose ``random()`` is
  ``f64_from_u64(random_u64)`` and whose ``getrandbits(k)`` concatenates ``random_u64``
  draws), ``os.urandom`` / ``random._urandom`` (and so ``random.SystemRandom`` and
  ``secrets``) → ``random-bytes``, ``uuid.uuid4`` → ``random-bytes(16)``. Explicitly seeded
  ``random.Random(seed)`` instances are untouched (they are deterministic already).
- **Time → logical** ``context.now``: ``time.time/time_ns/monotonic/monotonic_ns/
  perf_counter/perf_counter_ns``, argument-less ``time.localtime/gmtime/ctime/strftime``,
  and ``datetime.datetime.now/utcnow/today`` and ``datetime.date.today`` (the module's
  ``datetime``/``date`` classes are replaced by subclasses whose ``isinstance`` checks
  accept original instances).
- **stdio → log.emit**: ``sys.stdout`` lines are emitted at ``info``, ``sys.stderr`` at
  ``error``. Outside an activation output goes to the original stream, except inside a
  WASI guest, where it is dropped (a raw write traps under ``--stub-wasi``).
- **Finalizers:** a ``gc.callbacks`` guard flags collections; an SDK host call from a
  finalizer raises `ProtocolError` (finalizer timing differs live vs replay). `ostro.export()`
  calls ``gc.freeze()``.

Dict iteration is insertion-ordered (deterministic); ``set`` iteration over ``str``
depends on the build's hash seed (PYTHONHASHSEED does not reach pre-init, S9), so iterate
sets of strings through ``sorted()``.
"""

from __future__ import annotations

import datetime as _dt
import gc
import io
import os
import random
import secrets  # noqa: F401  (imported so its SystemRandom exists before patching)
import sys
import time
import uuid
from typing import Any

from . import loop as _loop
from .ctx import f64_from_u64

__all__ = ["HostRandom", "install", "installed"]

_installed = False
_MAX_RANDOM_BYTES = 65536

_orig_urandom = os.urandom
_orig_uuid4 = uuid.uuid4
_orig_time = time.time
_orig_time_ns = time.time_ns
_orig_monotonic = time.monotonic
_orig_monotonic_ns = time.monotonic_ns
_orig_perf_counter = time.perf_counter
_orig_perf_counter_ns = time.perf_counter_ns
_orig_localtime = time.localtime
_orig_gmtime = time.gmtime
_orig_ctime = time.ctime
_orig_strftime = time.strftime
_OrigDatetime = _dt.datetime
_OrigDate = _dt.date
_EPOCH = _OrigDatetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_UNSET: Any = object()


def installed() -> bool:
    return _installed


def _now_ns() -> int | None:
    lp = _loop.active()
    if lp is None:
        return None
    return lp._h(False).now_nanos()


def _host_bytes(n: int) -> bytes | None:
    lp = _loop.active()
    if lp is None:
        return None
    if n < 0:
        raise ValueError("negative byte count")
    h = lp._h(False)
    out = bytearray()
    while n > 0:
        k = min(n, _MAX_RANDOM_BYTES)
        out += h.random_bytes(k)
        n -= k
    return bytes(out)


# --- randomness ------------------------------------------------------------------------


class HostRandom(random.Random):
    """`random.Random` drawing from the host inside an activation."""

    def random(self) -> float:
        lp = _loop.active()
        if lp is None:
            return super().random()
        return f64_from_u64(lp._h(False).random_u64())

    def getrandbits(self, k: int) -> int:
        lp = _loop.active()
        if lp is None:
            return super().getrandbits(k)
        if k < 0:
            raise ValueError("number of bits must be non-negative")
        if k == 0:
            return 0
        h = lp._h(False)
        words = (k + 63) // 64
        x = 0
        for _ in range(words):
            x = (x << 64) | h.random_u64()
        return x >> (words * 64 - k)


def urandom(n: int) -> bytes:
    data = _host_bytes(n)
    return _orig_urandom(n) if data is None else data


def uuid4() -> uuid.UUID:
    data = _host_bytes(16)
    if data is None:
        return _orig_uuid4()
    return uuid.UUID(bytes=data, version=4)


def _patch_random() -> None:
    old = random._inst  # type: ignore[attr-defined]
    inst = HostRandom()
    for name in random.__all__:
        fn = getattr(random, name, None)
        if getattr(fn, "__self__", None) is old:
            setattr(random, name, getattr(inst, name))
    random._inst = inst  # type: ignore[attr-defined]
    random._urandom = urandom  # type: ignore[attr-defined]
    os.urandom = urandom
    uuid.uuid4 = uuid4


# --- time ------------------------------------------------------------------------------


def _time() -> float:
    ns = _now_ns()
    return _orig_time() if ns is None else ns / 1e9


def _time_ns() -> int:
    ns = _now_ns()
    return _orig_time_ns() if ns is None else ns


def _monotonic() -> float:
    ns = _now_ns()
    return _orig_monotonic() if ns is None else ns / 1e9


def _monotonic_ns() -> int:
    ns = _now_ns()
    return _orig_monotonic_ns() if ns is None else ns


def _perf_counter() -> float:
    ns = _now_ns()
    return _orig_perf_counter() if ns is None else ns / 1e9


def _perf_counter_ns() -> int:
    ns = _now_ns()
    return _orig_perf_counter_ns() if ns is None else ns


def _localtime(secs: Any = None) -> time.struct_time:
    if secs is None:
        ns = _now_ns()
        if ns is not None:
            return _orig_localtime(ns // 10**9)
    return _orig_localtime(secs)


def _gmtime(secs: Any = None) -> time.struct_time:
    if secs is None:
        ns = _now_ns()
        if ns is not None:
            return _orig_gmtime(ns // 10**9)
    return _orig_gmtime(secs)


def _ctime(secs: Any = None) -> str:
    if secs is None:
        ns = _now_ns()
        if ns is not None:
            return _orig_ctime(ns // 10**9)
    return _orig_ctime(secs)


def _strftime(fmt: str, t: Any = _UNSET) -> str:
    if t is _UNSET:
        ns = _now_ns()
        if ns is None:
            return _orig_strftime(fmt)
        t = _orig_localtime(ns // 10**9)
    return _orig_strftime(fmt, t)


class _OstroMeta(type):
    """`isinstance`/`issubclass` against the replaced classes accept the originals."""

    def __instancecheck__(cls, inst: object) -> bool:
        return isinstance(inst, cls.__ostro_base__)  # type: ignore[attr-defined]

    def __subclasscheck__(cls, sub: type) -> bool:
        return issubclass(sub, cls.__ostro_base__)  # type: ignore[attr-defined]


def _aware_now(ns: int, tz: _dt.tzinfo | None) -> _OrigDatetime:
    base = _EPOCH + _dt.timedelta(microseconds=ns // 1000)
    if tz is None:
        return base.astimezone().replace(tzinfo=None)
    return base.astimezone(tz)


class datetime(_OrigDatetime, metaclass=_OstroMeta):  # noqa: N801 (mirrors the stdlib name)
    __slots__ = ()
    __ostro_base__ = _OrigDatetime

    @classmethod
    def now(cls, tz: _dt.tzinfo | None = None) -> Any:
        ns = _now_ns()
        if ns is None:
            return super().now(tz)
        d = _aware_now(ns, tz)
        return cls(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, d.tzinfo, fold=d.fold)

    @classmethod
    def utcnow(cls) -> Any:
        ns = _now_ns()
        if ns is None:
            return cls.now(_dt.timezone.utc).replace(tzinfo=None)
        d = _aware_now(ns, _dt.timezone.utc)
        return cls(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)

    @classmethod
    def today(cls) -> Any:
        return cls.now()

    def __repr__(self) -> str:
        return "datetime." + super().__repr__()


class date(_OrigDate, metaclass=_OstroMeta):  # noqa: N801
    __slots__ = ()
    __ostro_base__ = _OrigDate

    @classmethod
    def today(cls) -> Any:
        ns = _now_ns()
        if ns is None:
            return super().today()
        d = _aware_now(ns, None)
        return cls(d.year, d.month, d.day)

    def __repr__(self) -> str:
        return "datetime." + super().__repr__()


for _cls in (datetime, date):
    _cls.__module__ = "datetime"


def _patch_time() -> None:
    time.time = _time
    time.time_ns = _time_ns
    time.monotonic = _monotonic
    time.monotonic_ns = _monotonic_ns
    time.perf_counter = _perf_counter
    time.perf_counter_ns = _perf_counter_ns
    time.localtime = _localtime
    time.gmtime = _gmtime
    time.ctime = _ctime
    time.strftime = _strftime
    _dt.datetime = datetime  # type: ignore[misc]
    _dt.date = date  # type: ignore[misc]


# --- stdio -----------------------------------------------------------------------------

_WASI = sys.platform == "wasi"


class _LogWriter(io.TextIOBase):
    """A text stream whose lines become ``log.emit`` calls inside an activation."""

    def __init__(self, level: str, fallback: Any):
        super().__init__()
        self._level = level
        self._fallback = fallback
        self._buf = ""

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return "utf-8"

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def _emit(self, lp: Any, line: str) -> None:
        line = line.encode("utf-8", "backslashreplace").decode("utf-8")
        try:
            lp._h(False).log_emit(self._level, line, [])
        except Exception:
            pass  # printing never fails a workflow

    def write(self, s: str) -> int:
        lp = _loop.active()
        if lp is None or _loop._gc_active:
            if self._fallback is not None and not _WASI:
                try:
                    return self._fallback.write(s)
                except Exception:
                    return len(s)
            return len(s)
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._emit(lp, line)
        return len(s)

    def flush(self) -> None:
        lp = _loop.active()
        if lp is not None and self._buf and not _loop._gc_active:
            line, self._buf = self._buf, ""
            self._emit(lp, line)
        elif lp is None and self._fallback is not None and not _WASI:
            try:
                self._fallback.flush()
            except Exception:
                pass


def _unwrap(stream: Any) -> Any:
    return stream._fallback if isinstance(stream, _LogWriter) else stream


def _stdio_enter() -> tuple[Any, Any, _LogWriter, _LogWriter]:
    out, err = sys.stdout, sys.stderr
    w_out = _LogWriter("info", _unwrap(out))
    w_err = _LogWriter("error", _unwrap(err))
    sys.stdout, sys.stderr = w_out, w_err
    return out, err, w_out, w_err


def _stdio_exit(state: tuple[Any, Any, _LogWriter, _LogWriter]) -> None:
    out, err, w_out, w_err = state
    w_out.flush()
    w_err.flush()
    if sys.stdout is w_out:
        sys.stdout = out
    if sys.stderr is w_err:
        sys.stderr = err


def _patch_stdio() -> None:
    sys.stdout = _LogWriter("info", _unwrap(sys.stdout))
    sys.stderr = _LogWriter("error", _unwrap(sys.stderr))
    _loop._activation_hooks.append((_stdio_enter, _stdio_exit))


# --- gc --------------------------------------------------------------------------------


def _gc_callback(phase: str, info: dict[str, Any]) -> None:
    _loop._gc_active = phase == "start"


def install() -> None:
    """Install every patch (idempotent). Called by ``import ostro``."""
    global _installed
    if _installed:
        return
    _installed = True
    _patch_random()
    _patch_time()
    _patch_stdio()
    gc.callbacks.append(_gc_callback)
