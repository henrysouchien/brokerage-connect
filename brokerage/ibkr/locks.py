"""Connection locks shared across IBKR modules."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
from types import TracebackType

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore[assignment]


class IBKRInterprocessLock:
    """Reentrant thread lock plus a process-wide file lock.

    TWS/Gateway permits only one active connection per client ID. Most local
    callers use fixed client IDs, so separate MCP/CLI processes need the same
    serialization that threads already had in-process.
    """

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        configured_path = path or os.getenv("IBKR_SHARED_LOCK_FILE")
        self.path = Path(configured_path) if configured_path else (
            Path(tempfile.gettempdir()) / "risk_module_ibkr_gateway.lock"
        )
        self._thread_lock = threading.RLock()
        self._local = threading.local()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        acquired = self._thread_lock.acquire(blocking, timeout)
        if not acquired:
            return False

        depth = int(getattr(self._local, "depth", 0))
        if depth == 0:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                fh = self.path.open("a+")
                if fcntl is not None:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                self._local.file_handle = fh
            except Exception:
                self._thread_lock.release()
                raise

        self._local.depth = depth + 1
        return True

    def release(self) -> None:
        depth = int(getattr(self._local, "depth", 0))
        if depth <= 0:
            raise RuntimeError("cannot release un-acquired IBKRInterprocessLock")

        if depth == 1:
            fh = getattr(self._local, "file_handle", None)
            try:
                if fh is not None and fcntl is not None:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                if fh is not None:
                    fh.close()
                self._local.file_handle = None

        self._local.depth = depth - 1
        self._thread_lock.release()

    def __enter__(self) -> "IBKRInterprocessLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


ibkr_shared_lock = IBKRInterprocessLock()
"""Serializes IBKR direct Gateway calls.

In persistent mode: serializes access to the shared IB instance.
In ephemeral mode: prevents concurrent connections on the same client ID,
including from separate local Python processes.
"""
