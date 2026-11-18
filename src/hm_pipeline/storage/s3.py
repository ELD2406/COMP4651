from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hm_pipeline.checkpoints.manifest import sha256_file
from hm_pipeline.common.exceptions import ConfigurationError, StorageError
from hm_pipeline.config.settings import Settings

LOGGER = logging.getLogger(__name__)


def count_csv_rows(path: Path) -> int:
    import csv

    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        next(reader, None)
        return sum(1 for _ in reader)


class RawStorage:
    """Upload exact source files to local storage or S3, with checksum manifests."""

    def __init__(self, settings: Settings, s3_client: Any | None = None):
        self.settings = settings
        self.s3_client = s3_client
        if settings.storage_backend == "s3" and s3_client is None:
            try:
                import boto3
            except ImportError as exc:
                raise ConfigurationError("S3 support requires the 's3' extra: pip install -e .[s3]") from exc
            self.s3_client = boto3.session.Session().client("s3")

    def upload(self, source: Path, dataset: str, row_count: int | None = None) -> dict[str, Any]:
        if not source.is_file():
            raise StorageError(f"Source file does not exist: {source}")
        checksum = sha256_file(source)
        size = source.stat().st_size
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        key = "/".join(part.strip("/") for part in (self.settings.s3_prefix, dataset, source.name) if part.strip("/"))
        record = {
            "source_filename": source.name,
            "ingested_at": timestamp,
            "file_size_bytes": size,
            "sha256": checksum,
            "row_count": row_count,
            "schema_version": 1,
            "ingestion_status": "completed",
            "destination": key,
        }
        if self.settings.storage_backend == "local":
            destination = self.settings.local_storage_root / key
            self._upload_local(source, destination, checksum)
            record["destination"] = str(destination)
            self._update_manifest(key, record)
            return record
        if not self.settings.s3_bucket:
            raise StorageError("S3 backend selected but storage.bucket / AWS_S3_BUCKET is empty")
        self._upload_s3(source, key, checksum, timestamp, row_count)
        self._update_manifest(key, record)
        return record

    def _upload_local(self, source: Path, destination: Path, checksum: str) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.is_file() and sha256_file(destination) == checksum:
            LOGGER.info("Raw object already present with matching checksum: %s", destination)
            return
        temporary = destination.with_name(destination.name + ".partial")
        try:
            with source.open("rb") as src, temporary.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
                dst.flush()
                os.fsync(dst.fileno())
            if sha256_file(temporary) != checksum:
                raise StorageError(f"Checksum verification failed for local copy {destination}")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def _upload_s3(self, source: Path, key: str, checksum: str, timestamp: str, row_count: int | None) -> None:
        client = self.s3_client
        bucket = self.settings.s3_bucket
        try:
            try:
                existing = client.head_object(Bucket=bucket, Key=key)
                metadata = existing.get("Metadata", {})
                if existing.get("ContentLength") == source.stat().st_size and metadata.get("sha256") == checksum:
                    LOGGER.info("S3 object already present with matching checksum: s3://%s/%s", bucket, key)
                    return
            except Exception as exc:
                error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if error_code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
            metadata = {
                "sha256": checksum,
                "source-filename": source.name,
                "ingested-at": timestamp,
                "schema-version": "1",
                "ingestion-status": "completed",
            }
            if row_count is not None:
                metadata["row-count"] = str(row_count)
            client.upload_file(
                str(source), bucket, key,
                ExtraArgs={"Metadata": metadata, "ContentType": "text/csv"},
            )
            verified = client.head_object(Bucket=bucket, Key=key)
            if verified.get("ContentLength") != source.stat().st_size:
                raise StorageError(f"S3 size verification failed for s3://{bucket}/{key}")
            if verified.get("Metadata", {}).get("sha256") != checksum:
                raise StorageError(f"S3 checksum metadata verification failed for s3://{bucket}/{key}")
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(f"S3 upload failed for s3://{bucket}/{key}: {exc}") from exc

    def _update_manifest(self, key: str, record: dict[str, Any]) -> None:
        path = self.settings.s3_manifest_path
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            manifest = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"Cannot read S3 upload manifest {path}: {exc}") from exc
        manifest[key] = record
        fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(manifest, stream, indent=2, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
