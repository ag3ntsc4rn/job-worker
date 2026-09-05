"""Runtime configuration, read from the environment.

Defaults point at the docker-compose stack in this repo, so ``python -m worker``
works locally with no flags.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    database_url: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    # all replicas share one group, so partitions are split between them
    consumer_group: str
    # seconds one poll waits for a message before the loop checks for shutdown
    poll_timeout: float
    # retry (per dependency call, before the failure reaches that breaker)
    retry_max_attempts: int
    retry_backoff_base: float
    retry_backoff_max: float
    # circuit breakers (one per dependency, same settings)
    breaker_failure_threshold: int
    breaker_reset_timeout: float
    # how long the loop pauses after a breaker rejects a call
    breaker_open_sleep: float
    log_level: str
    # job types routed to the generic structured-completion handler
    llm_job_types: tuple[str, ...]
    llm_default_provider: str
    llm_default_model: str
    llm_timeout: float
    # a provider with no key is simply not configured
    openai_api_key: str | None
    openai_base_url: str | None
    anthropic_api_key: str | None
    anthropic_base_url: str | None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        get = (env if env is not None else os.environ).get
        return cls(
            database_url=get("DATABASE_URL", "postgresql://app:app@localhost:5432/app"),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=get("KAFKA_TOPIC", "jobs"),
            consumer_group=get("CONSUMER_GROUP", "workers"),
            poll_timeout=float(get("WORKER_POLL_TIMEOUT", "1.0")),
            retry_max_attempts=int(get("RETRY_MAX_ATTEMPTS", "3")),
            retry_backoff_base=float(get("RETRY_BACKOFF_BASE", "0.2")),
            retry_backoff_max=float(get("RETRY_BACKOFF_MAX", "5.0")),
            breaker_failure_threshold=int(get("BREAKER_FAILURE_THRESHOLD", "5")),
            breaker_reset_timeout=float(get("BREAKER_RESET_TIMEOUT", "30.0")),
            breaker_open_sleep=float(get("BREAKER_OPEN_SLEEP", "5.0")),
            log_level=get("LOG_LEVEL", "INFO"),
            llm_job_types=tuple(
                t.strip() for t in get("LLM_JOB_TYPES", "llm_structured").split(",") if t.strip()
            ),
            llm_default_provider=get("LLM_DEFAULT_PROVIDER", "openai"),
            llm_default_model=get("LLM_DEFAULT_MODEL", "gpt-4.1-mini"),
            llm_timeout=float(get("LLM_TIMEOUT", "60.0")),
            openai_api_key=get("OPENAI_API_KEY") or None,
            openai_base_url=get("OPENAI_BASE_URL") or None,
            anthropic_api_key=get("ANTHROPIC_API_KEY") or None,
            anthropic_base_url=get("ANTHROPIC_BASE_URL") or None,
        )
