"""Kafka-backed ``Consumer``.

Auto-commit is off: the loop commits explicitly once the run has been recorded,
which is what keeps delivery at-least-once rather than at-most-once. This module
needs a real broker, so it is exercised by docker-compose rather than the unit
suite and is excluded from coverage.
"""

from __future__ import annotations

import json
from typing import Any


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
        return json.loads(msg.value().decode("utf-8"))

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
