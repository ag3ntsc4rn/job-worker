from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _quiet_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Keep the retry/breaker warnings the failure tests provoke out of the output."""
    caplog.set_level(logging.CRITICAL, logger="worker")
