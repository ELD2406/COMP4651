from __future__ import annotations

import logging
import json
import time
from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from hm_pipeline.common.exceptions import ConfigurationError, DeliveryError
from hm_pipeline.config.settings import Settings

LOGGER = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish_batch(self, events: Iterable[Mapping[str, Any]]) -> int: ...
    def close(self) -> None: ...


class KafkaPublisher:
    def __init__(self, settings: Settings, producer: Any | None = None):
        if producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise ConfigurationError("Kafka support requires the 'kafka' extra: pip install -e .[kafka]") from exc
            producer = Producer({
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": settings.kafka_acks,
                "enable.idempotence": True,
                "retries": 2147483647,
                "max.in.flight.requests.per.connection": 5,
                "compression.type": settings.kafka_compression_type,
                "linger.ms": settings.kafka_linger_ms,
                "batch.size": settings.kafka_batch_size_bytes,
                "request.timeout.ms": settings.kafka_request_timeout_ms,
            })
        self.producer = producer
        self.topic = settings.kafka_topic

    def publish_batch(self, events: Iterable[Mapping[str, Any]]) -> int:
        failures: list[str] = []
        queued = 0
        for event in events:
            value = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            key = str(event["customer_id"]).encode("utf-8")
            event_id = event["event_id"]

            def delivered(error: Any, message: Any, event_id: str = event_id) -> None:
                if error is not None:
                    failures.append(str(error))
                elif message is not None:
                    LOGGER.debug("Kafka acknowledged %s at %s[%s]@%s", event_id, message.topic(), message.partition(), message.offset())

            while True:
                try:
                    self.producer.produce(self.topic, key=key, value=value, on_delivery=delivered)
                    queued += 1
                    break
                except BufferError:
                    self.producer.poll(0.5)
            self.producer.poll(0)
        outstanding = self.producer.flush()
        if outstanding:
            failures.append(f"{outstanding} messages remained unacknowledged after flush")
        if failures:
            raise DeliveryError("Kafka delivery failed: " + "; ".join(failures[:5]))
        return queued

    def close(self) -> None:
        outstanding = self.producer.flush()
        if outstanding:
            raise DeliveryError(f"Kafka shutdown left {outstanding} messages unacknowledged")


class RecordingPublisher:
    """In-memory publisher for tests and local validation; sends no network traffic."""

    def __init__(self):
        self.events: list[dict[str, Any]] = []
        self.keys: list[str] = []

    def publish_batch(self, events: Iterable[Mapping[str, Any]]) -> int:
        batch = [dict(event) for event in events]
        self.events.extend(batch)
        self.keys.extend(str(event["customer_id"]) for event in batch)
        return len(batch)

    def close(self) -> None:
        return None
