"""Where the actual work goes.

A handler is any callable taking an :class:`Envelope`. Returning marks the run
``completed``; raising marks it ``failed``. The default handler does no
processing at all and always succeeds, so the service is useful as-is (it moves
jobs to ``completed``) and a real deployment only has to swap this out.

Adding one
----------

1. Write the function here. Everything it needs is on the envelope
   (``job_id``, ``job_type``, ``payload``) or in :class:`worker.config.Config`::

       def send_report(envelope: Envelope) -> None:
           recipient = envelope.payload["recipient"]        # KeyError -> failed
           report = build_report(envelope.job_id)
           email.send(recipient, report, timeout=30)        # bounded, see below

2. Wire it in ``worker/__main__.py`` in place of ``always_succeeds``. For more
   than one job type, dispatch on ``envelope.job_type`` -- see ``by_job_type``.
3. Test it through ``process()`` against ``InMemoryJobStore`` (see
   ``tests/test_service.py``); the handler contract *is* the status transition,
   so asserting on ``store.status_of(job_id)`` is the assertion that matters.

What a handler must be
----------------------

* **Idempotent.** Delivery is at-least-once and the reaper re-queues stranded
  runs, so the same job can reach a handler more than once. Key side effects on
  ``envelope.job_id`` (an upsert, a conditional insert, an idempotency key on the
  outbound API call) rather than assuming one delivery.
* **Bounded.** A handler that hangs holds the run in ``running`` until the
  reaper's ``run_timeout`` reclaims it, which stalls that job type in the
  meantime. Pass explicit timeouts to every network call you make.

What raising means
------------------

Raising is *terminal*: the run is marked ``failed`` and nothing retries it
automatically -- the next schedule enqueues a fresh run. So raise for a genuine
business failure (bad input, rejected by a downstream system), and for a
transient one prefer either an in-handler retry or letting the run stay
``running`` for the reaper by raising :class:`worker.resilience.CircuitOpenError`
from a guarded dependency. ``process()`` deliberately lets that one through
without marking the run failed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping

from worker.models import Envelope

logger = logging.getLogger(__name__)

Handler = Callable[[Envelope], None]


def always_succeeds(envelope: Envelope) -> None:
    """Generic no-op handler: records the run and reports success.

    Replace this with real work, or route to it per type with ``by_job_type``.
    """
    logger.info("handling job %s (%s): no-op", envelope.job_id, envelope.job_type)


def by_job_type(handlers: Mapping[str, Handler], default: Handler = always_succeeds) -> Handler:
    """Compose per-type handlers into the single handler the loop calls.

    ::

        run(store, consumer, by_job_type({"send_report": send_report}))

    An unmapped type falls through to ``default``, which succeeds rather than
    failing the run: several worker deployments can then share one topic, each
    handling its own types and no-op'ing the rest. Pass a ``default`` that raises
    if this deployment owns every type on the topic and an unknown one should be
    loud instead.
    """

    def dispatch(envelope: Envelope) -> None:
        handler = handlers.get(envelope.job_type, default)
        handler(envelope)

    return dispatch
