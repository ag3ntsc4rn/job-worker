"""The processing logic — pure, so it unit-tests against any store/consumer."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from worker.consumer import Consumer
from worker.handlers import Handler
from worker.models import Envelope, MalformedEnvelope
from worker.resilience import CircuitOpenError
from worker.store import JobStore

logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    malformed: int = 0

    @property
    def processed(self) -> int:
        return self.completed + self.failed + self.skipped + self.malformed


def process(store: JobStore, handler: Handler, message: dict[str, Any]) -> str:
    """Claim, run, and record one message. Returns the outcome.

    Outcomes: ``"malformed"`` (not a job pointer), ``"skipped"`` (lost the claim,
    so this is a redelivery or another worker owns the run), ``"completed"``, or
    ``"failed"`` (the handler raised).

    Claiming *before* running is the whole safety property: the status
    compare-and-set is the only thing standing between at-least-once delivery
    and running a job twice. The claim also returns the run's effective payload
    (type config overlaid with the run's input), which is why the handler is
    called with the envelope the claim produced rather than the parsed one.
    """
    try:
        envelope = Envelope.parse(message)
    except MalformedEnvelope:
        # Nothing to claim and no job to fail; retrying forever would wedge the
        # partition, so the caller commits past it.
        logger.exception("dropping unparseable message")
        return "malformed"

    payload = store.claim(envelope.job_id)
    if payload is None:
        logger.info("job %s already claimed; skipping redelivery", envelope.job_id)
        return "skipped"

    try:
        # The claim resolved the run's config; an empty payload is a job type
        # that needs none, which is why the check above is `is None`.
        handler(envelope.with_payload(payload))
    except CircuitOpenError:
        raise  # a dependency is down, not the job's fault: do not mark it failed
    except Exception:
        logger.exception("handler for job %s (%s) raised", envelope.job_id, envelope.job_type)
        store.fail(envelope.job_id)
        return "failed"

    store.complete(envelope.job_id)
    logger.info("job %s (%s) completed", envelope.job_id, envelope.job_type)
    return "completed"


def run(
    store: JobStore,
    consumer: Consumer,
    handler: Handler,
    *,
    poll_timeout: float,
    breaker_open_sleep: float,
    should_stop: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
) -> WorkerStats:
    """Consume until ``should_stop()``. Returns the totals.

    The offset is committed only after the run is recorded. A crash in between
    redelivers the message and the claim guard skips it; committing first would
    silently drop the job instead.

    The loop never dies on a dependency failure: retries and the per-dependency
    circuit breakers live in the wrappers around ``store``/``consumer``, and an
    uncommitted message is simply redelivered.
    """
    stats = WorkerStats()
    while not should_stop():
        try:
            message = consumer.poll(poll_timeout)
            if message is None:
                continue
            outcome = process(store, handler, message)
            consumer.commit()
        except CircuitOpenError as err:
            # Uncommitted, so the message comes back once the breaker closes.
            logger.warning("circuit open, backing off: %s", err)
            sleep(breaker_open_sleep)
            continue
        except Exception:
            logger.exception("processing failed; message left uncommitted")
            sleep(breaker_open_sleep)
            continue
        if outcome == "completed":
            stats.completed += 1
        elif outcome == "failed":
            stats.failed += 1
        elif outcome == "skipped":
            stats.skipped += 1
        else:
            stats.malformed += 1
    logger.info(
        "shutting down after %d completed, %d failed, %d skipped",
        stats.completed,
        stats.failed,
        stats.skipped,
    )
    return stats
