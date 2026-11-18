from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def sha256_file(path: Path, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_size):
            digest.update(block)
    return digest.hexdigest()


class CheckpointStore:
    """Atomic JSON checkpoints keyed by source and replay-window identity."""

    def __init__(self, path: Path):
        self.path = path

    def load(self, source_identity: str, window: str) -> int:
        if not self.path.exists():
            return 0
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read checkpoint {self.path}: {exc}") from exc
        if data.get("source_identity") != source_identity or data.get("window") != window:
            raise ValueError("Checkpoint belongs to a different source file or replay window")
        return int(data.get("last_acked_row", 0))

    def save(self, source_identity: str, window: str, last_acked_row: int) -> None:
        if last_acked_row < 0:
            raise ValueError("last_acked_row cannot be negative")
        data: dict[str, Any] = {
            "source_identity": source_identity,
            "window": window,
            "last_acked_row": last_acked_row,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f"{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(data, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_name, self.path)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)
