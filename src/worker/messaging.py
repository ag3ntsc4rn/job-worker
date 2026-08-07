"""Kafka-backed ``Consumer``.

Decoding lives here, but a decode failure is deliberately *not* reported as a
broker failure: it raises :class:`MalformedEnvelope`, which the ``kafka-source``
guard neither retries nor counts, so garbage on the topic cannot open a breaker
on a healthy broker.

Auto-commit is off: the loop commits explicitly once the run has been recorded,
which is what keeps delivery at-least-once rather than at-most-once. This module
needs a real broker, so it is exercised by docker-compose rather than the unit
suite and is excluded from coverage.
"""

from __future__ import annotations

import json
from typing import Any

from worker.models import MalformedEnvelope


class KafkaConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str) -> None:
        from confluent_kafka import Consumer as ConfluentConsumer

        self._consumer = ConfluentConsumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def poll(self, timeout: float) -> dict[str, Any] | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"kafka poll failed: {msg.error()}")
        raw = msg.value()
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            # Bytes that are not JSON are a producer bug, not a broker fault, so
            # they are raised as the same error an unparseable envelope raises:
            # the guard lets it through unretried and the loop commits past it.
            raise MalformedEnvelope(f"undecodable message: {raw!r}") from err

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
