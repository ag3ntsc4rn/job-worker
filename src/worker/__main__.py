"""Worker entrypoint: ``python -m worker``.

Wires the two real dependencies, each behind its own guard, and consumes until
SIGTERM/SIGINT.
"""

from __future__ import annotations

import logging

from worker.config import Config
from worker.consumer import GuardedConsumer
from worker.db import PostgresJobStore
from worker.handlers import always_succeeds
from worker.messaging import KafkaConsumer
from worker.service import run
from worker.shutdown import ShutdownSignal
from worker.store import GuardedJobStore
from worker.wiring import build_consumer_guard, build_store_guard

logger = logging.getLogger(__name__)


def main() -> int:
    cfg = Config.from_env()
    # Only the entrypoint touches handlers; embedders configure logging themselves.
    logging.basicConfig(level=cfg.log_level.upper())

    store = GuardedJobStore(PostgresJobStore(cfg.database_url), build_store_guard(cfg))
    consumer = GuardedConsumer(
        KafkaConsumer(cfg.kafka_bootstrap_servers, cfg.consumer_group, cfg.kafka_topic),
        build_consumer_guard(cfg),
    )

    shutdown = ShutdownSignal()
    shutdown.install()
    logger.info(
        "worker started, topic=%s group=%s", cfg.kafka_topic, cfg.consumer_group
    )
    try:
        run(
            store,
            consumer,
            always_succeeds,
            poll_timeout=cfg.poll_timeout,
            breaker_open_sleep=cfg.breaker_open_sleep,
            should_stop=shutdown,
        )
    finally:
        consumer.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
