from __future__ import annotations

import csv
import logging
import time
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

from hm_pipeline.checkpoints.manifest import CheckpointStore, sha256_file
from hm_pipeline.common.exceptions import ValidationError
from hm_pipeline.kafka.producer import Publisher
from hm_pipeline.schemas.transaction import TRANSACTION_FIELDS, transaction_event, validate_header, validate_transaction

LOGGER = logging.getLogger(__name__)


def _rows(path: Path) -> Iterator[tuple[int, dict[str, str | None]]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, restkey="__extra__", restval=None, strict=True)
        validate_header(reader.fieldnames or [], TRANSACTION_FIELDS, path.name)
        previous_date: date | None = None
        for row_number, row in enumerate(reader, start=1):
            if row.get("__extra__"):
                raise ValidationError(f"Transaction row {row_number} contains extra columns")
            current_date, _, _ = validate_transaction(row, row_number)
            if previous_date is not None and current_date < previous_date:
                raise ValidationError(f"Transaction dates are not ordered at source row {row_number}")
            previous_date = current_date
            yield row_number, row


def validate_transactions(path: Path, limit: int | None = None) -> int:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    count = 0
    for _ in _rows(path):
        count += 1
        if limit is not None and count >= limit:
            break
    return count


def replay_transactions(
    path: Path,
    publisher: Publisher,
    checkpoint: CheckpointStore,
    start_date: date,
    end_date: date,
    batch_size: int = 5000,
    speed: float = 0.0,
    checkpoint_every_rows: int | None = None,
    limit: int | None = None,
) -> int:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    if batch_size <= 0 or speed < 0:
        raise ValueError("batch_size must be positive and speed cannot be negative")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    checkpoint_rows = min(checkpoint_every_rows or batch_size, batch_size)
    if checkpoint_rows <= 0:
        raise ValueError("checkpoint_every_rows must be positive")

    identity = sha256_file(path)
    window = f"{start_date.isoformat()}..{end_date.isoformat()}"
    last_acked = checkpoint.load(identity, window)
    pending_events: list[dict] = []
    sent = 0
    since_checkpoint = 0
    started = time.monotonic()
    scanned = last_acked
    try:
        for row_number, row in _rows(path):
            if row_number <= last_acked:
                continue
            event_date = date.fromisoformat(str(row["t_dat"]))
            if event_date > end_date:
                break
            scanned = row_number
            since_checkpoint += 1
            if start_date <= event_date <= end_date:
                event = transaction_event(row, row_number, identity)
                pending_events.append(event.__dict__)

            if speed and pending_events:
                time.sleep(1.0 / speed)
            if limit is not None and sent + len(pending_events) >= limit:
                sent += publisher.publish_batch(pending_events)
                pending_events.clear()
                checkpoint.save(identity, window, scanned)
                last_acked = scanned
                break
            if since_checkpoint >= checkpoint_rows:
                if pending_events:
                    sent += publisher.publish_batch(pending_events)
                    pending_events.clear()
                checkpoint.save(identity, window, scanned)
                last_acked = scanned
                since_checkpoint = 0
                if scanned % (checkpoint_rows * 10) == 0:
                    elapsed = max(time.monotonic() - started, 0.001)
                    LOGGER.info("Replay progress: scanned=%d published=%d rate=%.1f events/s", scanned, sent, sent / elapsed)

        if pending_events:
            sent += publisher.publish_batch(pending_events)
        if scanned > last_acked:
            checkpoint.save(identity, window, scanned)
        elapsed = max(time.monotonic() - started, 0.001)
        LOGGER.info("Replay complete: scanned through source row %d, published=%d, rate=%.1f events/s", scanned, sent, sent / elapsed)
        return sent
    finally:
        publisher.close()
