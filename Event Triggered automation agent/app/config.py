from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    webhook_secret: str = "change-me"
    admin_token: str = ""

    n8n_webhook_url: str = "http://localhost:5678/webhook/events"
    n8n_timeout_seconds: float = 10.0
    groq_api_key: str = ""
    groq_model: str = "llama-3.1-8b-instant"

    max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 30.0

    idempotency_ttl_seconds: int = 86400
    processing_lock_seconds: int = 300

    stream_key: str = "events:stream"
    dlq_key: str = "events:dlq"
    consumer_group: str = "workers"
    stream_maxlen: int = 10000

    db_path: str = "data/audit.db"


@lru_cache
def get_settings() -> Settings:
    return Settings()