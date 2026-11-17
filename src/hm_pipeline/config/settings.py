from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from hm_pipeline.common.exceptions import ConfigurationError


@dataclass(frozen=True)
class Settings:
    dataset_root: Path
    checkpoint_dir: Path
    transactions_file: Path | None = None
    customers_file: Path | None = None
    articles_file: Path | None = None
    log_level: str = "INFO"
    batch_size: int = 5000
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "fashion.purchase-lines.v1"
    kafka_topic_partitions: int | None = None
    kafka_acks: str = "all"
    kafka_compression_type: str = "snappy"
    kafka_linger_ms: int = 20
    kafka_batch_size_bytes: int = 131072
    kafka_request_timeout_ms: int = 30000
    replay_speed: float = 0.0
    replay_start_date: date | None = None
    replay_end_date: date | None = None
    checkpoint_every_rows: int = 5000
    storage_backend: str = "local"
    local_storage_root: Path = Path("raw-data")
    s3_bucket: str = ""
    s3_prefix: str = "raw"
    s3_manifest_path: Path = Path(".checkpoints/s3-manifest.json")

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Settings":
        data: dict[str, Any] = {}
        if config_path:
            try:
                with config_path.open("rb") as stream:
                    data = tomllib.load(stream)
            except OSError as exc:
                raise ConfigurationError(f"Cannot read config {config_path}: {exc}") from exc
            except tomllib.TOMLDecodeError as exc:
                raise ConfigurationError(f"Invalid TOML config {config_path}: {exc}") from exc

        pipeline = data.get("pipeline", {})
        kafka = data.get("kafka", {})
        replay = data.get("replay", {})
        sources = data.get("sources", {})
        storage = data.get("storage", {})
        repo_root = Path(__file__).resolve().parents[3]
        values: dict[str, Any] = {
            "dataset_root": pipeline.get("dataset_root", repo_root / "h-and-m-personalized-fashion-recommendations"),
            "checkpoint_dir": pipeline.get("checkpoint_dir", Path(".checkpoints")),
            "transactions_file": sources.get("transactions"),
            "customers_file": sources.get("customers"),
            "articles_file": sources.get("articles"),
            "log_level": pipeline.get("log_level", "INFO"),
            "batch_size": pipeline.get("batch_size", 5000),
            "kafka_bootstrap_servers": kafka.get("bootstrap_servers", "localhost:9092"),
            "kafka_topic": kafka.get("topic", "fashion.purchase-lines.v1"),
            "kafka_topic_partitions": kafka.get("topic_partitions"),
            "kafka_acks": kafka.get("acks", "all"),
            "kafka_compression_type": kafka.get("compression_type", "snappy"),
            "kafka_linger_ms": kafka.get("linger_ms", 20),
            "kafka_batch_size_bytes": kafka.get("batch_size_bytes", 131072),
            "kafka_request_timeout_ms": kafka.get("request_timeout_ms", 30000),
            "replay_speed": replay.get("speed", 0.0),
            "replay_start_date": replay.get("start_date"),
            "replay_end_date": replay.get("end_date"),
            "checkpoint_every_rows": replay.get("checkpoint_every_rows", 5000),
            "storage_backend": storage.get("backend", "local"),
            "local_storage_root": storage.get("local_root", Path("raw-data")),
            "s3_bucket": storage.get("bucket", ""),
            "s3_prefix": storage.get("prefix", "raw"),
            "s3_manifest_path": storage.get("manifest_path", Path(".checkpoints/s3-manifest.json")),
        }
        env = {
            "dataset_root": ("HM_DATASET_ROOT", Path),
            "checkpoint_dir": ("HM_CHECKPOINT_DIR", Path),
            "transactions_file": ("HM_TRANSACTIONS_FILE", Path),
            "customers_file": ("HM_CUSTOMERS_FILE", Path),
            "articles_file": ("HM_ARTICLES_FILE", Path),
            "log_level": ("HM_LOG_LEVEL", str),
            "batch_size": ("HM_BATCH_SIZE", int),
            "kafka_bootstrap_servers": ("KAFKA_BOOTSTRAP_SERVERS", str),
            "kafka_topic": ("KAFKA_TOPIC", str),
            "kafka_topic_partitions": ("KAFKA_TOPIC_PARTITIONS", int),
            "kafka_acks": ("KAFKA_ACKS", str),
            "kafka_compression_type": ("KAFKA_COMPRESSION_TYPE", str),
            "kafka_linger_ms": ("KAFKA_LINGER_MS", int),
            "kafka_batch_size_bytes": ("KAFKA_BATCH_SIZE_BYTES", int),
            "kafka_request_timeout_ms": ("KAFKA_REQUEST_TIMEOUT_MS", int),
            "replay_speed": ("HM_REPLAY_SPEED", float),
            "replay_start_date": ("HM_REPLAY_START_DATE", date.fromisoformat),
            "replay_end_date": ("HM_REPLAY_END_DATE", date.fromisoformat),
            "checkpoint_every_rows": ("HM_CHECKPOINT_EVERY_ROWS", int),
            "storage_backend": ("HM_STORAGE_BACKEND", str),
            "local_storage_root": ("HM_LOCAL_STORAGE_ROOT", Path),
            "s3_bucket": ("AWS_S3_BUCKET", str),
            "s3_prefix": ("AWS_S3_PREFIX", str),
            "s3_manifest_path": ("HM_S3_MANIFEST_PATH", Path),
        }
        try:
            for key, (env_name, convert) in env.items():
                raw = os.environ.get(env_name)
                if raw is not None:
                    values[key] = convert(raw)
                elif convert in (int, float):
                    if values[key] is not None:
                        values[key] = convert(values[key])
            for key in ("dataset_root", "checkpoint_dir", "local_storage_root", "s3_manifest_path"):
                values[key] = Path(values[key]).expanduser()
            for key in ("transactions_file", "customers_file", "articles_file"):
                if values[key] is not None:
                    values[key] = Path(values[key]).expanduser()
            for key in ("replay_start_date", "replay_end_date"):
                if values[key] is not None and not isinstance(values[key], date):
                    values[key] = date.fromisoformat(str(values[key]))
            settings = cls(**values)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"Invalid configuration value: {exc}") from exc
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.batch_size <= 0 or self.checkpoint_every_rows <= 0:
            raise ConfigurationError("batch_size and checkpoint_every_rows must be positive")
        if self.replay_speed < 0:
            raise ConfigurationError("replay speed cannot be negative")
        if (self.replay_start_date is None) != (self.replay_end_date is None):
            raise ConfigurationError("replay start_date and end_date must be configured together")
        if self.replay_start_date and self.replay_start_date > self.replay_end_date:
            raise ConfigurationError("replay start_date must be on or before end_date")
        if self.storage_backend not in {"local", "s3"}:
            raise ConfigurationError("storage backend must be 'local' or 's3'")
        if not self.kafka_topic:
            raise ConfigurationError("Kafka topic cannot be empty")
        if self.kafka_topic_partitions is not None and self.kafka_topic_partitions <= 0:
            raise ConfigurationError("Kafka topic_partitions must be positive when set")

    def source_path(self, filename: str) -> Path:
        configured = {
            "transactions_train.csv": self.transactions_file,
            "customers.csv": self.customers_file,
            "articles.csv": self.articles_file,
        }.get(filename)
        path = configured if configured is not None else self.dataset_root / filename
        if not path.is_absolute():
            path = self.dataset_root / path
        if not path.is_file():
            raise ConfigurationError(f"Dataset file not found: {path}")
        return path
