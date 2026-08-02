"""The message contract the worker shares with the dispatcher."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class MalformedEnvelope(ValueError):
    """A message that is not a job pointer; retrying it would never help."""


@dataclass(frozen=True)
class Envelope:
    """One job message: a pointer, not the work itself.

    Keeping the payload out of the message is what lets the job row stay the
    single source of truth — a redelivery from three hours ago still runs
    against current data rather than a stale copy baked into Kafka.
    """

    job_id: int
    job_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, message: dict[str, Any]) -> Envelope:
        try:
            return cls(
                job_id=int(message["job_id"]),
                job_type=str(message["job_type"]),
                payload=message.get("payload") or {},
            )
        except (KeyError, TypeError, ValueError) as err:
            raise MalformedEnvelope(f"not a job envelope: {message!r}") from err
