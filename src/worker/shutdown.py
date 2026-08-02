"""Graceful shutdown: turn SIGTERM/SIGINT into a flag the loop polls.

Killing the worker mid-job is safe — the offset is only committed after the run
is recorded, so an interrupted message is redelivered and the claim guard sorts
out the duplicate — but the loop still finishes the job in hand rather than
dying where it stands.
"""

from __future__ import annotations

import signal
from types import FrameType


class ShutdownSignal:
    def __init__(self) -> None:
        self._requested = False

    def install(self, signals: tuple[int, ...] = (signal.SIGTERM, signal.SIGINT)) -> None:
        for sig in signals:
            signal.signal(sig, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        self._requested = True

    def request(self) -> None:
        self._requested = True

    @property
    def requested(self) -> bool:
        return self._requested

    def __call__(self) -> bool:
        return self._requested
