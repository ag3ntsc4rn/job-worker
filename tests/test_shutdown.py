from __future__ import annotations

import os
import signal

from worker.shutdown import ShutdownSignal


def test_starts_unset_and_flips_on_request():
    shutdown = ShutdownSignal()

    assert shutdown() is False
    shutdown.request()
    assert shutdown() is True and shutdown.requested is True


def test_sigterm_requests_shutdown_without_killing_the_process():
    shutdown = ShutdownSignal()
    original = signal.getsignal(signal.SIGTERM)
    try:
        shutdown.install(signals=(signal.SIGTERM,))
        os.kill(os.getpid(), signal.SIGTERM)
        assert shutdown.requested is True
    finally:
        signal.signal(signal.SIGTERM, original)
