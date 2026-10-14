from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    redis_url: str = "redis://redis:6379/0"
    internal_api_key: str = "dev-internal-key"

    ad_min_interval_sec: int = 8
    ad_max_interval_sec: int = 15
    session_expiry_buffer_sec: int = 60

    enter_rate_limit_requests: int = 30
    enter_rate_limit_window_sec: int = 60

    log_level: str = "INFO"


settings = Settings()
