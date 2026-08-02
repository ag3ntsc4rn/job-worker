"""Where the actual work goes.

A handler is any callable taking an :class:`Envelope`. Returning marks the run
``completed``; raising marks it ``failed``. The default handler does no
processing at all and always succeeds, so the service is useful as-is (it moves
jobs to ``completed``) and a real deployment only has to swap this out.

Two properties any replacement needs:

* **Idempotent.** Delivery is at-least-once and the reaper re-queues stranded
  runs, so the same job can reach a handler more than once.
* **Bounded.** A handler that hangs holds the run in ``running`` until the
  reaper's ``run_timeout`` reclaims it; use your own timeouts rather than
  relying on that.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from worker.models import Envelope

logger = logging.getLogger(__name__)

Handler = Callable[[Envelope], None]


def always_succeeds(envelope: Envelope) -> None:
    """Generic no-op handler: records the run and reports success."""
    logger.info("handling job %s (%s): no-op", envelope.job_id, envelope.job_type)
