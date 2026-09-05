"""Where the actual work goes.

A handler is any callable taking an :class:`Envelope`. Returning marks the run
``completed`` -- and a non-``None`` return value is stored on the row as its
JSON ``result`` -- while raising marks it ``failed``. The entrypoint routes on
:data:`HANDLERS`, so a job type this deployment has not registered fails rather
than quietly reporting success -- see :func:`unhandled`. Out of the box only the
demo type ``hello`` is registered, on a no-op handler.

Adding one
----------

1. Write the function here. Per-run configuration is on ``envelope.payload``,
   resolved at claim time as the type's ``job_type_config.payload`` overlaid
   with the run's ``jobs.input_payload``; per-deployment settings (URLs,
   credentials) belong in :class:`worker.config.Config`::

       def send_report(envelope: Envelope) -> None:
           recipient = envelope.payload["recipient"]        # KeyError -> failed
           report = build_report(envelope.job_id)
           email.send(recipient, report, timeout=30)        # bounded, see below

2. Register it in :data:`HANDLERS` under the ``job_type`` it serves. The
   entrypoint routes on that map, so nothing else has to change::

       HANDLERS = {"hello": always_succeeds, "send_report": send_report}

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
from typing import Any

from worker.models import Envelope

logger = logging.getLogger(__name__)

Handler = Callable[[Envelope], Any]


def always_succeeds(envelope: Envelope) -> None:
    """Generic no-op handler: records the run and reports success."""
    logger.info("handling job %s (%s): no-op", envelope.job_id, envelope.job_type)


class UnknownJobType(LookupError):
    """No handler is registered for the run's ``job_type``."""


def unhandled(envelope: Envelope) -> None:
    """Fall-through for a type with no handler: fail the run.

    Every type reaching this worker is one it is expected to own, so an unmapped
    type is a bug — a typo'd ``job_type``, or a handler nobody registered — and
    completing the run would report success for work that never happened.

    Pass a ``default`` that returns instead if this ``jobs`` table is ever shared
    with a second worker deployment handling a disjoint set of types; there,
    failing would destroy the other deployment's runs.
    """
    logger.error("no handler for job type %r; failing job %s", envelope.job_type, envelope.job_id)
    raise UnknownJobType(envelope.job_type)


HANDLERS: dict[str, Handler] = {
    # The types this deployment owns. Registering one here is all routing needs;
    # anything else fails through `unhandled`. 'hello' is the compose demo type,
    # kept on the no-op handler so a fresh stack still moves jobs to completed.
    "hello": always_succeeds,
}


def by_job_type(handlers: Mapping[str, Handler], default: Handler = unhandled) -> Handler:
    """Compose per-type handlers into the single handler the loop calls.

    ::

        run(store, consumer, by_job_type({"send_report": send_report}))

    An unmapped type falls through to ``default`` — :func:`unhandled`, which
    fails the run.
    """

    def dispatch(envelope: Envelope) -> Any:
        handler = handlers.get(envelope.job_type, default)
        return handler(envelope)

    return dispatch
