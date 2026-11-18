from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Mapping

from hm_pipeline.common.exceptions import ValidationError

TRANSACTION_FIELDS = ("t_dat", "customer_id", "article_id", "price", "sales_channel_id")
CUSTOMER_FIELDS = (
    "customer_id", "FN", "Active", "club_member_status", "fashion_news_frequency", "age", "postal_code"
)
ARTICLE_FIELDS = (
    "article_id", "product_code", "prod_name", "product_type_no", "product_type_name", "product_group_name",
    "graphical_appearance_no", "graphical_appearance_name", "colour_group_code", "colour_group_name",
    "perceived_colour_value_id", "perceived_colour_value_name", "perceived_colour_master_id",
    "perceived_colour_master_name", "department_no", "department_name", "index_code", "index_name",
    "index_group_no", "index_group_name", "section_no", "section_name", "garment_group_no",
    "garment_group_name", "detail_desc",
)


@dataclass(frozen=True)
class TransactionEvent:
    event_id: str
    schema_version: int
    source_dataset: str
    source_file: str
    source_row_number: int
    event_date: str
    ingested_at: str
    customer_id: str
    article_id: str
    price: str
    sales_channel_id: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), ensure_ascii=False)


def validate_header(actual: list[str] | tuple[str, ...], expected: tuple[str, ...], source: str) -> None:
    if tuple(actual) != expected:
        raise ValidationError(f"{source} header mismatch: expected {expected}, got {tuple(actual)}")


def validate_transaction(row: Mapping[str, str | None], row_number: int) -> tuple[date, Decimal, int]:
    missing = [field for field in TRANSACTION_FIELDS if row.get(field) is None or row.get(field) == ""]
    if missing:
        raise ValidationError(f"Transaction row {row_number} missing required fields: {', '.join(missing)}")
    customer_id = str(row["customer_id"])
    article_id = str(row["article_id"])
    if not re.fullmatch(r"[0-9a-f]{64}", customer_id):
        raise ValidationError(f"Transaction row {row_number} has an invalid customer_id")
    if not re.fullmatch(r"\d{10}", article_id):
        raise ValidationError(f"Transaction row {row_number} has an invalid article_id")
    try:
        event_date = date.fromisoformat(str(row["t_dat"]))
        price = Decimal(str(row["price"]))
        channel = int(str(row["sales_channel_id"]))
    except (ValueError, InvalidOperation) as exc:
        raise ValidationError(f"Transaction row {row_number} contains an invalid date, price, or channel") from exc
    if not price.is_finite() or price < 0:
        raise ValidationError(f"Transaction row {row_number} has an invalid price")
    return event_date, price, channel


def transaction_event(
    row: Mapping[str, str | None], row_number: int, source_identity: str,
    ingested_at: datetime | None = None,
) -> TransactionEvent:
    event_date, price, channel = validate_transaction(row, row_number)
    event_id = hashlib.sha256(f"{source_identity}:{row_number}".encode("utf-8")).hexdigest()
    stamp = ingested_at or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        raise ValidationError("ingested_at must be timezone-aware")
    stamp = stamp.astimezone(timezone.utc)
    return TransactionEvent(
        event_id=event_id,
        schema_version=1,
        source_dataset="transactions_train",
        source_file="transactions_train.csv",
        source_row_number=row_number,
        event_date=event_date.isoformat(),
        ingested_at=stamp.isoformat(timespec="seconds").replace("+00:00", "Z"),
        customer_id=str(row["customer_id"]),
        article_id=str(row["article_id"]),
        price=str(price),
        sales_channel_id=channel,
    )
