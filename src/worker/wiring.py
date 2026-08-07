"""Builds the guards the entrypoint puts around each dependency.

Kept out of ``__main__`` so the wiring is unit-tested rather than only
exercised in production.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence

from worker.config import Config
from worker.models import MalformedEnvelope
from worker.resilience import Guard, RetryPolicy, build_breaker

logger = logging.getLogger(__name__)


def build_guard(
    name: str,
    cfg: Config,
    *,
    sleep: Callable[[float], None] = time.sleep,
    non_transient: Sequence[type[BaseException]] = (),
) -> Guard:
    """``non_transient`` errors are neither retried nor counted as failures."""
    return Guard(
        build_breaker(
            name,
            failure_threshold=cfg.breaker_failure_threshold,
            reset_timeout=cfg.breaker_reset_timeout,
            exclude=non_transient,
        ),
        RetryPolicy(
            max_attempts=cfg.retry_max_attempts,
            base_delay=cfg.retry_backoff_base,
            max_delay=cfg.retry_backoff_max,
        ),
        sleep=sleep,
        retryable=lambda err: not isinstance(err, tuple(non_transient)),
        on_retry=lambda dep, attempt, delay, err: logger.warning(
            "%s attempt %d failed, retrying in %.2fs: %s", dep, attempt, delay, err
        ),
    )


def build_store_guard(cfg: Config, *, sleep: Callable[[float], None] = time.sleep) -> Guard:
    return build_guard("postgres-jobs", cfg, sleep=sleep)


def build_consumer_guard(cfg: Config, *, sleep: Callable[[float], None] = time.sleep) -> Guard:
    # A message the transport cannot decode says nothing about the broker's
    # health: retrying it only burns backoff, and counting it would open this
    # breaker over someone else's bad publish.
    return build_guard("kafka-source", cfg, sleep=sleep, non_transient=(MalformedEnvelope,))
