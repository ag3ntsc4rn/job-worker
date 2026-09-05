"""Worker entrypoint: ``python -m worker``.

Wires the two real dependencies, each behind its own guard, and consumes until
SIGTERM/SIGINT.
"""

from __future__ import annotations

import logging

from worker.config import Config
from worker.consumer import GuardedConsumer
from worker.db import PostgresJobStore
from worker.handlers import HANDLERS, Handler, by_job_type
from worker.llm import LLMClient
from worker.llm_providers import AnthropicClient, OpenAIClient
from worker.messaging import KafkaConsumer
from worker.service import run
from worker.shutdown import ShutdownSignal
from worker.store import GuardedJobStore
from worker.wiring import build_consumer_guard, build_llm_handler, build_store_guard

logger = logging.getLogger(__name__)


def llm_clients(cfg: Config) -> dict[str, LLMClient]:
    clients: dict[str, LLMClient] = {}
    if cfg.openai_api_key:
        clients["openai"] = OpenAIClient(
            cfg.openai_api_key, base_url=cfg.openai_base_url, timeout=cfg.llm_timeout
        )
    if cfg.anthropic_api_key:
        clients["anthropic"] = AnthropicClient(
            cfg.anthropic_api_key, base_url=cfg.anthropic_base_url, timeout=cfg.llm_timeout
        )
    return clients


def main() -> int:
    cfg = Config.from_env()
    # Only the entrypoint touches handlers; embedders configure logging themselves.
    logging.basicConfig(level=cfg.log_level.upper())

    store = GuardedJobStore(PostgresJobStore(cfg.database_url), build_store_guard(cfg))
    handlers: dict[str, Handler] = dict(HANDLERS)
    llm_handler = build_llm_handler(cfg, llm_clients(cfg))
    if llm_handler is None:
        logger.warning("no LLM provider configured; job types %s will fail", cfg.llm_job_types)
    else:
        handlers.update(dict.fromkeys(cfg.llm_job_types, llm_handler))
    consumer = GuardedConsumer(
        KafkaConsumer(cfg.kafka_bootstrap_servers, cfg.consumer_group, cfg.kafka_topic),
        build_consumer_guard(cfg),
    )

    shutdown = ShutdownSignal()
    shutdown.install()
    logger.info(
        "worker started, topic=%s group=%s, handling %s",
        cfg.kafka_topic,
        cfg.consumer_group,
        sorted(handlers),
    )
    try:
        run(
            store,
            consumer,
            by_job_type(handlers),
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
