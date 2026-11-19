from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from hm_pipeline.checkpoints.manifest import CheckpointStore
from hm_pipeline.common.exceptions import ConfigurationError, ValidationError
from hm_pipeline.config.settings import Settings
from hm_pipeline.ingestion.discovery import discover_source
from hm_pipeline.ingestion.references import iter_reference_chunks, validate_reference_file
from hm_pipeline.ingestion.transactions import replay_transactions, validate_transactions
from hm_pipeline.kafka.producer import KafkaPublisher, RecordingPublisher
from hm_pipeline.schemas.transaction import (
    ARTICLE_FIELDS, CUSTOMER_FIELDS, TRANSACTION_FIELDS, transaction_event,
    validate_header, validate_transaction,
)
from hm_pipeline.storage.s3 import RawStorage

CUSTOMER = "0" * 64


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def sample_transactions() -> list[dict[str, str]]:
    return [
        {"t_dat": "2019-01-01", "customer_id": CUSTOMER, "article_id": "0108775015", "price": "0.050", "sales_channel_id": "1"},
        {"t_dat": "2019-01-01", "customer_id": CUSTOMER, "article_id": "0108775044", "price": "1.25", "sales_channel_id": "2"},
        {"t_dat": "2019-01-02", "customer_id": CUSTOMER, "article_id": "0108775051", "price": "2.00", "sales_channel_id": "1"},
        {"t_dat": "2019-01-03", "customer_id": CUSTOMER, "article_id": "0108775051", "price": "3.00", "sales_channel_id": "2"},
    ]


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def transaction_file(self, rows=None):
        return write_csv(self.root / "transactions_train.csv", TRANSACTION_FIELDS, rows or sample_transactions())

    def test_discovery_and_unknown_source(self):
        (self.root / "customers.csv").write_text("customer_id\n", encoding="utf-8")
        self.assertEqual(discover_source(self.root, "customers").name, "customers.csv")
        with self.assertRaises(ConfigurationError):
            discover_source(self.root, "articles")
        with self.assertRaises(ConfigurationError):
            discover_source(self.root, "pos")

    def test_settings_toml_and_environment_override(self):
        config = self.root / "config.toml"
        config.write_text(
            f'[pipeline]\nbatch_size=9\ndataset_root="{self.root.as_posix()}"\n'
            '[sources]\ntransactions="custom.csv"\n'
            '[kafka]\ntopic_partitions=4\n'
            '[replay]\nstart_date=2020-01-01\nend_date=2020-01-02\n'
            '[storage]\nbackend="local"\n',
            encoding="utf-8",
        )
        (self.root / "custom.csv").write_text("", encoding="utf-8")
        settings = Settings.load(config)
        self.assertEqual(settings.batch_size, 9)
        self.assertEqual(settings.storage_backend, "local")
        self.assertEqual(settings.source_path("transactions_train.csv"), self.root / "custom.csv")
        self.assertEqual(settings.replay_start_date, date(2020, 1, 1))
        self.assertEqual(settings.kafka_topic_partitions, 4)
        with patch.dict("os.environ", {"HM_BATCH_SIZE": "12"}):
            self.assertEqual(Settings.load(config).batch_size, 12)

    def test_transaction_decimal_ids_and_json(self):
        row = sample_transactions()[0]
        event = transaction_event(row, 1, "source-hash", datetime(2026, 11, 17, tzinfo=timezone.utc))
        self.assertEqual(event.article_id, "0108775015")
        self.assertEqual(event.price, "0.050")
        self.assertEqual(validate_transaction(row, 1), (date(2019, 1, 1), Decimal("0.050"), 1))
        self.assertIn('"schema_version":1', event.to_json())
        self.assertIn('"event_date":"2019-01-01"', event.to_json())
        same = transaction_event(row, 1, "source-hash", datetime(2026, 11, 17, tzinfo=timezone.utc))
        other = transaction_event(row, 2, "source-hash", datetime(2026, 11, 17, tzinfo=timezone.utc))
        self.assertEqual(event.event_id, same.event_id)
        self.assertNotEqual(event.event_id, other.event_id)

    def test_invalid_transaction_and_header(self):
        row = dict(sample_transactions()[0], price="NaN")
        with self.assertRaises(ValidationError):
            validate_transaction(row, 4)
        with self.assertRaises(ValidationError):
            validate_header(["date"], TRANSACTION_FIELDS, "test")

    def test_reference_chunks_preserve_nullable_values_and_ids(self):
        customer = {field: "" for field in CUSTOMER_FIELDS}
        customer.update(customer_id="a" * 64, postal_code="hash")
        path = write_csv(self.root / "customers.csv", CUSTOMER_FIELDS, [customer, customer | {"customer_id": "b" * 64}])
        chunks = list(iter_reference_chunks(path, "customers", 1))
        self.assertEqual([len(chunk) for chunk in chunks], [1, 1])
        self.assertEqual(chunks[0][0]["FN"], "")
        self.assertEqual(validate_reference_file(path, "customers", 10), 2)

        article = {field: "" for field in ARTICLE_FIELDS}
        article.update(
            article_id="0108775015", product_code="0108775", prod_name="Top",
            product_type_no="1", graphical_appearance_no="1", perceived_colour_value_id="1",
            perceived_colour_master_id="1", department_no="1", index_group_no="1", section_no="1",
            garment_group_no="1",
        )
        article_path = write_csv(self.root / "articles.csv", ARTICLE_FIELDS, [article])
        self.assertEqual(next(iter_reference_chunks(article_path, "articles", 10))[0]["article_id"], "0108775015")
        article["article_id"] = "bad"
        write_csv(article_path, ARTICLE_FIELDS, [article])
        with self.assertRaises(ValidationError):
            list(iter_reference_chunks(article_path, "articles", 10))

    def test_replay_window_source_order_and_customer_keys(self):
        path = self.transaction_file()
        publisher = RecordingPublisher()
        sent = replay_transactions(
            path, publisher, CheckpointStore(self.root / "checkpoint.json"),
            date(2019, 1, 1), date(2019, 1, 2), batch_size=2,
        )
        self.assertEqual(sent, 3)
        self.assertEqual([event["source_row_number"] for event in publisher.events], [1, 2, 3])
        self.assertEqual(publisher.keys, [event["customer_id"] for event in publisher.events])
        self.assertTrue(all("event_date" in event and "occurred_at" not in event for event in publisher.events))

    def test_replay_limit_bounds_each_run_and_resumes_from_checkpoint(self):
        path = self.transaction_file()
        checkpoint = CheckpointStore(self.root / "limited-checkpoint.json")
        publisher = RecordingPublisher()
        sent = replay_transactions(
            path, publisher, checkpoint,
            date(2019, 1, 1), date(2019, 1, 3), batch_size=4, limit=2,
        )
        self.assertEqual(sent, 2)
        self.assertEqual([event["source_row_number"] for event in publisher.events], [1, 2])
        resumed = RecordingPublisher()
        self.assertEqual(replay_transactions(
            path, resumed, checkpoint,
            date(2019, 1, 1), date(2019, 1, 3), batch_size=4, limit=2,
        ), 2)
        self.assertEqual([event["source_row_number"] for event in resumed.events], [3, 4])

    def test_checkpoint_resume_and_failure(self):
        path = self.transaction_file()
        checkpoint = CheckpointStore(self.root / "checkpoint.json")
        options = dict(checkpoint=checkpoint, start_date=date(2019, 1, 1), end_date=date(2019, 1, 1), batch_size=2)
        self.assertEqual(replay_transactions(path, RecordingPublisher(), **options), 2)
        self.assertEqual(replay_transactions(path, RecordingPublisher(), **options), 0)

        checkpoint.path.unlink()

        class FailingPublisher:
            def publish_batch(self, events):
                raise RuntimeError("delivery failed")
            def close(self):
                pass

        with self.assertRaises(RuntimeError):
            replay_transactions(path, FailingPublisher(), **options)
        self.assertFalse(checkpoint.path.exists())

    def test_transaction_validation_checks_sorting(self):
        rows = sample_transactions()
        path = self.transaction_file([rows[2], rows[0]])
        with self.assertRaises(ValidationError):
            validate_transactions(path)

    def test_kafka_uses_customer_key_and_waits_for_delivery_ack(self):
        settings = Settings(dataset_root=self.root, checkpoint_dir=self.root)
        acknowledged = []

        class Message:
            def topic(self): return settings.kafka_topic
            def partition(self): return 0
            def offset(self): return 42

        class FakeProducer:
            def __init__(self): self.queue = []; self.history = []
            def produce(self, topic, key, value, on_delivery):
                record = (topic, key, json.loads(value), on_delivery)
                self.queue.append(record)
                self.history.append(record)
            def poll(self, timeout): return 0
            def flush(self):
                queued, self.queue = self.queue, []
                for _, _, event, callback in queued:
                    callback(None, Message())
                    acknowledged.append(event["event_id"])
                return 0

        producer = FakeProducer()
        publisher = KafkaPublisher(settings, producer)
        event = {"event_id": "event-1", "customer_id": "customer-1", "article_id": "0108775015"}
        self.assertEqual(publisher.publish_batch([event]), 1)
        self.assertEqual(acknowledged, ["event-1"])
        self.assertEqual(producer.history[0][:3], (settings.kafka_topic, b"customer-1", event))

    def test_local_storage_verifies_and_skips_identical_copy(self):
        source = self.root / "customers.csv"
        source.write_text("customer_id\nabc\n", encoding="utf-8")
        settings = Settings(dataset_root=self.root, checkpoint_dir=self.root / "checkpoints",
                            local_storage_root=self.root / "raw", s3_manifest_path=self.root / "manifest.json")
        storage = RawStorage(settings)
        first = storage.upload(source, "customers", 1)
        second = storage.upload(source, "customers", 1)
        target = self.root / "raw" / "raw" / "customers" / "customers.csv"
        self.assertEqual(target.read_bytes(), source.read_bytes())
        self.assertEqual(first["sha256"], second["sha256"])
        manifest = json.loads((self.root / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["raw/customers/customers.csv"]["row_count"], 1)

    def test_s3_upload_integrity_metadata_prevents_repeat_upload(self):
        source = self.root / "articles.csv"
        source.write_text("article_id\n0108775015\n", encoding="utf-8")
        settings = Settings(dataset_root=self.root, checkpoint_dir=self.root / "checkpoints",
                            storage_backend="s3", s3_bucket="test-bucket",
                            s3_manifest_path=self.root / "manifest.json")

        class NotFound(Exception):
            response = {"Error": {"Code": "404"}}

        class Client:
            def __init__(self): self.object = None; self.uploads = 0
            def head_object(self, Bucket, Key):
                if self.object is None: raise NotFound()
                return self.object
            def upload_file(self, filename, bucket, key, ExtraArgs):
                self.uploads += 1
                self.object = {"ContentLength": source.stat().st_size, "Metadata": ExtraArgs["Metadata"]}

        client = Client()
        storage = RawStorage(settings, s3_client=client)
        storage.upload(source, "articles", 1)
        storage.upload(source, "articles", 1)
        self.assertEqual(client.uploads, 1)


if __name__ == "__main__":
    unittest.main()
