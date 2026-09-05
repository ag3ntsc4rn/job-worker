"""Builds the guards the entrypoint puts around each dependency.

Kept out of ``__main__`` so the wiring is unit-tested rather than only
exercised in production.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence

from worker.config import Config
from worker.handlers import Handler
from worker.llm import (
    BadPayload,
    GuardedLLMClient,
    InvalidOutput,
    LLMClient,
    structured_completion,
)
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


def build_llm_guard(
    provider: str, cfg: Config, *, sleep: Callable[[float], None] = time.sleep
) -> Guard:
    # A reply that fails the schema, or a payload we cannot send, says nothing
    # about whether the provider is up: the handler owns that retry, not the guard.
    return build_guard(
        f"llm-{provider}", cfg, sleep=sleep, non_transient=(InvalidOutput, BadPayload)
    )


def build_llm_handler(
    cfg: Config,
    clients: Mapping[str, LLMClient],
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> Handler | None:
    """The ``llm_structured`` handler with each provider behind its own guard.

    ``None`` when no provider is configured, so a deployment without LLM keys
    simply does not register the LLM job types (and fails them via ``unhandled``
    rather than crashing at startup).
    """
    if not clients:
        return None
    if cfg.llm_default_provider not in clients:
        raise ValueError(
            f"LLM_DEFAULT_PROVIDER={cfg.llm_default_provider!r} has no API key configured; "
            f"configured providers: {sorted(clients)}"
        )
    guarded = {
        name: GuardedLLMClient(client, build_llm_guard(name, cfg, sleep=sleep))
        for name, client in clients.items()
    }
    return structured_completion(
        guarded,
        default_provider=cfg.llm_default_provider,
        default_model=cfg.llm_default_model,
    )
