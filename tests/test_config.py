from __future__ import annotations

from worker.config import Config


def test_defaults_point_at_the_compose_stack():
    cfg = Config.from_env({})

    assert cfg.database_url == "postgresql://app:app@localhost:5432/app"
    assert cfg.kafka_bootstrap_servers == "localhost:9092"
    assert (cfg.kafka_topic, cfg.consumer_group) == ("jobs", "workers")
    assert cfg.poll_timeout == 1.0
    assert cfg.retry_max_attempts == 3
    assert (cfg.breaker_failure_threshold, cfg.breaker_reset_timeout) == (5, 30.0)
    assert cfg.breaker_open_sleep == 5.0
    assert cfg.log_level == "INFO"


def test_environment_overrides_are_coerced_to_their_types():
    cfg = Config.from_env(
        {
            "DATABASE_URL": "postgresql://u:p@db:5432/jobs",
            "KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
            "KAFKA_TOPIC": "work",
            "CONSUMER_GROUP": "reports",
            "WORKER_POLL_TIMEOUT": "0.25",
            "RETRY_MAX_ATTEMPTS": "7",
            "RETRY_BACKOFF_BASE": "0.05",
            "RETRY_BACKOFF_MAX": "2.5",
            "BREAKER_FAILURE_THRESHOLD": "9",
            "BREAKER_RESET_TIMEOUT": "12.5",
            "BREAKER_OPEN_SLEEP": "3.5",
            "LOG_LEVEL": "DEBUG",
        }
    )

    assert cfg.database_url == "postgresql://u:p@db:5432/jobs"
    assert cfg.kafka_bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert (cfg.kafka_topic, cfg.consumer_group) == ("work", "reports")
    assert cfg.poll_timeout == 0.25
    assert (cfg.retry_max_attempts, cfg.retry_backoff_base, cfg.retry_backoff_max) == (
        7,
        0.05,
        2.5,
    )
    assert (cfg.breaker_failure_threshold, cfg.breaker_reset_timeout) == (9, 12.5)
    assert cfg.breaker_open_sleep == 3.5
    assert cfg.log_level == "DEBUG"
