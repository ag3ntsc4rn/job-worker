"""The message contract the worker shares with the dispatcher."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any


class MalformedEnvelope(ValueError):
    """A message that is not a job pointer; retrying it would never help."""


@dataclass(frozen=True)
class Envelope:
    """One job message: a pointer, not the work itself.

    ``payload`` is *not* read off the message — it is resolved from the database
    when the run is claimed (see :meth:`with_payload`). Keeping it out of the
    message is what lets the job row stay the single source of truth: a
    redelivery from three hours ago runs against current config rather than a
    stale copy baked into Kafka.
    """

    job_id: int
    job_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, message: dict[str, Any]) -> Envelope:
        try:
            return cls(job_id=int(message["job_id"]), job_type=str(message["job_type"]))
        except (KeyError, TypeError, ValueError) as err:
            raise MalformedEnvelope(f"not a job envelope: {message!r}") from err

    def with_payload(self, payload: dict[str, Any]) -> Envelope:
        """This envelope with the effective payload resolved at claim time."""
        return replace(self, payload=payload)
