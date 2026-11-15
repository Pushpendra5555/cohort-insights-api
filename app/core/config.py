"""Environment-based configuration.

Every tunable lives here and is overridable via environment variables (or a
``.env`` file). See ``.env.example`` for the documented list.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- service -----------------------------------------------------------
    app_name: str = "cohort-insights-api"
    environment: str = "local"
    log_level: str = "INFO"

    # --- infrastructure ----------------------------------------------------
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "cohort_insights"
    mongo_server_selection_timeout_ms: int = 5000
    redis_url: str = "redis://localhost:6379/0"
    redis_socket_timeout_s: float = 0.5
    # Prefix for every Redis key; isolates environments / test runs sharing one Redis.
    redis_namespace: str = "cia"

    # --- rate limiting -----------------------------------------------------
    max_active_docs_per_user: int = Field(default=3, ge=1)
    # Upper bound on how long a leaked slot can block a user (self-healing).
    rate_limit_slot_ttl_s: int = Field(default=3600, ge=60)

    # --- content cache -----------------------------------------------------
    cache_ttl_s: int = Field(default=86_400, ge=1)
    # Bump when the summarisation/tagging algorithm changes: invalidates the cache.
    pipeline_version: str = "v1"

    # --- simulated pipeline ------------------------------------------------
    stage1_min_s: float = Field(default=10.0, ge=0)
    stage1_max_s: float = Field(default=20.0, ge=0)
    stage2_min_s: float = Field(default=5.0, ge=0)
    stage2_max_s: float = Field(default=15.0, ge=0)
    stage_failure_rate: float = Field(default=0.10, ge=0, le=1)
    stage_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_s: float = Field(default=2.0, ge=0)
    retry_max_delay_s: float = Field(default=30.0, ge=0)

    # --- worker ------------------------------------------------------------
    lease_duration_s: float = Field(default=60.0, gt=0)
    worker_concurrency: int = Field(default=4, ge=1)
    worker_poll_interval_s: float = Field(default=0.5, gt=0)

    # --- API ---------------------------------------------------------------
    default_page_size: int = Field(default=20, ge=1)
    max_page_size: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def _check_consistency(self) -> "Settings":
        if self.stage1_min_s > self.stage1_max_s or self.stage2_min_s > self.stage2_max_s:
            raise ValueError("stage *_min_s must be <= *_max_s")
        longest_stage = max(self.stage1_max_s, self.stage2_max_s)
        if self.lease_duration_s <= longest_stage:
            # A lease shorter than a stage would let a second worker steal live work.
            raise ValueError("lease_duration_s must exceed the longest simulated stage")
        if self.default_page_size > self.max_page_size:
            raise ValueError("default_page_size must be <= max_page_size")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
