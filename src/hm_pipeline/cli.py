from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import date
from pathlib import Path

from hm_pipeline.common.exceptions import ConfigurationError, PipelineError
from hm_pipeline.common.logging import configure_logging
from hm_pipeline.config.settings import Settings
from hm_pipeline.checkpoints.manifest import CheckpointStore
from hm_pipeline.ingestion.discovery import discover_source
from hm_pipeline.ingestion.references import validate_reference_file
from hm_pipeline.ingestion.transactions import replay_transactions, validate_transactions
from hm_pipeline.kafka.producer import KafkaPublisher
from hm_pipeline.storage.s3 import RawStorage, count_csv_rows

LOGGER = logging.getLogger("hm_pipeline.cli")

SOURCE_OVERRIDES = {
    "transactions": "transactions_file",
    "customers": "customers_file",
    "articles": "articles_file",
}


def _source(settings: Settings, name: str) -> Path:
    return discover_source(settings.dataset_root, name, getattr(settings, SOURCE_OVERRIDES[name]))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream, validate, and archive H&M source datasets.")
    parser.add_argument("--config", type=Path, help="TOML configuration file (environment variables override it)")
    commands = parser.add_subparsers(dest="command", required=True)

    customers = commands.add_parser("ingest-customers", help="Validate and archive the customer snapshot")
    customers.add_argument("--chunk-size", type=int)
    articles = commands.add_parser("ingest-articles", help="Validate and archive the article snapshot")
    articles.add_argument("--chunk-size", type=int)
    upload = commands.add_parser("upload-raw-transactions", help="Archive the original transactions CSV to raw storage")
    upload.add_argument("--count-rows", action="store_true", help="Scan the full file to include its row count in metadata")
    validate = commands.add_parser("validate-transactions", help="Stream and validate transaction records")
    validate.add_argument("--limit", type=int, help="Validate at most this many rows; default scans the whole file")
    replay = commands.add_parser("replay-transactions", help="Replay a bounded date window to Kafka")
    replay.add_argument("--start-date", type=date.fromisoformat, help="Inclusive ISO date YYYY-MM-DD (required unless set in config)")
    replay.add_argument("--end-date", type=date.fromisoformat, help="Inclusive ISO date YYYY-MM-DD (required unless set in config)")
    replay.add_argument("--speed", type=float, help="Events per second; 0 (default) runs without artificial delay")
    replay.add_argument("--batch-size", type=int, help="Maximum source rows per acknowledgement/checkpoint batch")
    config = commands.add_parser("inspect-config", help="Print effective non-secret pipeline configuration")
    config.set_defaults(show_config=True)
    return parser


def run(args: argparse.Namespace) -> int:
    settings = Settings.load(args.config)
    configure_logging(settings.log_level)
    if args.command == "inspect-config":
        safe = asdict(settings)
        safe["dataset_root"] = str(safe["dataset_root"])
        for key in ("checkpoint_dir", "local_storage_root", "s3_manifest_path"):
            safe[key] = str(safe[key])
        print(json.dumps(safe, indent=2, default=str))
        return 0

    if args.command in {"ingest-customers", "ingest-articles"}:
        dataset = "customers" if args.command == "ingest-customers" else "articles"
        path = _source(settings, dataset)
        count = validate_reference_file(path, dataset, args.chunk_size or settings.batch_size)
        record = RawStorage(settings).upload(path, dataset, count)
        LOGGER.info("Validated and archived %s rows from %s", count, path)
        print(json.dumps(record, indent=2))
        return 0

    if args.command == "upload-raw-transactions":
        path = _source(settings, "transactions")
        rows = count_csv_rows(path) if args.count_rows else None
        record = RawStorage(settings).upload(path, "transactions", rows)
        LOGGER.info("Archived raw transaction source %s", path)
        print(json.dumps(record, indent=2))
        return 0

    if args.command == "validate-transactions":
        if args.limit is not None and args.limit <= 0:
            raise ValueError("--limit must be positive")
        count = validate_transactions(_source(settings, "transactions"), args.limit)
        LOGGER.info("Validated %d transaction rows", count)
        print(f"Validated {count} transaction rows")
        return 0

    if args.command == "replay-transactions":
        path = _source(settings, "transactions")
        start_date = args.start_date or settings.replay_start_date
        end_date = args.end_date or settings.replay_end_date
        if start_date is None or end_date is None:
            raise ConfigurationError("Set replay start/end dates in config or pass --start-date and --end-date")
        publisher = KafkaPublisher(settings)
        checkpoint_path = settings.checkpoint_dir / f"transactions-{start_date}-{end_date}.json"
        published = replay_transactions(
            path=path,
            publisher=publisher,
            checkpoint=CheckpointStore(checkpoint_path),
            start_date=start_date,
            end_date=end_date,
            batch_size=args.batch_size or settings.batch_size,
            speed=settings.replay_speed if args.speed is None else args.speed,
            checkpoint_every_rows=settings.checkpoint_every_rows,
        )
        print(f"Published {published} events to {settings.kafka_topic}")
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (PipelineError, ValueError) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
