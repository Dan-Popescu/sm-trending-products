"""Validated deployment settings. Secret values never appear in repr or status."""

from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", populate_by_name=True)

    database_url: SecretStr
    s3_bucket: str = "sm-trends"
    s3_endpoint_url: str | None = None
    aws_default_region: str = "us-east-1"
    cache_dir: Path = Path(".cache/objects")
    spool_dir: Path = Path(".cache/spool")
    cache_max_bytes: int = Field(default=2_000_000_000, ge=1_000_000)
    collector_binary: str = "sm-collector"
    worker_id: str = ""
    pipeline_namespace: str = Field(default="development", pattern=r"^[A-Za-z0-9_-]+$")
    batch_size: int = Field(default=20, ge=1, le=1000)
    lease_seconds: int = Field(default=180, ge=30)
    max_attempts: int = Field(default=8, ge=1, le=100)
    collection_max_pages: int = Field(default=1000, ge=1, le=100000)
    analysis_product_batch_size: int = Field(default=100, ge=1, le=1000)
    analysis_max_rows_per_batch: int = Field(default=1_000_000, ge=100)
    history_partition_buckets: int = Field(default=64, ge=1, le=1024)
    history_index_objects_per_call: int = Field(default=32, ge=1, le=1000)
    history_target_rows_per_file: int = Field(default=1000, ge=1, le=100_000)
    collector_timeout_seconds: int = Field(default=90, ge=5, le=600)
    polars_max_threads: int = Field(default=2, ge=1)
    duckdb_memory_limit: str = "512MB"
    duckdb_threads: int = Field(default=2, ge=1, le=64)

    @model_validator(mode="after")
    def validate_endpoints(self) -> "Settings":
        if self.s3_endpoint_url is not None and not self.s3_endpoint_url.strip():
            self.s3_endpoint_url = None
        if not self.database_url.get_secret_value().startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must use PostgreSQL")
        if "/" in self.s3_bucket or not self.s3_bucket:
            raise ValueError("S3_BUCKET must be a bucket name")
        if self.collector_timeout_seconds + 30 >= self.lease_seconds:
            raise ValueError("Lease must exceed collector timeout plus publication headroom")
        return self
