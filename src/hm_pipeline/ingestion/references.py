from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

from hm_pipeline.common.exceptions import ValidationError
from hm_pipeline.schemas.transaction import ARTICLE_FIELDS, CUSTOMER_FIELDS, validate_header


def iter_reference_chunks(
    path: Path, dataset: str, chunk_size: int
) -> Iterator[list[dict[str, str | None]]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    fields, key = (CUSTOMER_FIELDS, "customer_id") if dataset == "customers" else (ARTICLE_FIELDS, "article_id")
    if dataset not in {"customers", "articles"}:
        raise ValueError(f"Unsupported reference dataset: {dataset}")
    chunk: list[dict[str, str | None]] = []
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, restkey="__extra__", restval=None, strict=True)
        validate_header(reader.fieldnames or [], fields, dataset)
        for row_number, row in enumerate(reader, start=1):
            if row.get("__extra__") or any(row.get(field) is None for field in fields) or row.get(key) == "":
                raise ValidationError(f"{dataset} row {row_number} has extra columns or an empty {key}")
            if dataset == "articles" and not (row[key].isdigit() and len(row[key]) == 10):
                raise ValidationError(f"articles row {row_number} has an invalid article_id")
            if dataset == "articles":
                numeric_fields = (
                    "product_type_no", "graphical_appearance_no", "perceived_colour_value_id",
                    "perceived_colour_master_id", "department_no", "index_group_no", "section_no", "garment_group_no",
                )
                if any(not row[field].isdigit() for field in numeric_fields):
                    raise ValidationError(f"articles row {row_number} has an invalid numeric code")
            if dataset == "customers" and (len(row[key]) != 64 or any(c not in "0123456789abcdef" for c in row[key])):
                raise ValidationError(f"customers row {row_number} has an invalid customer_id")
            if dataset == "customers" and row["age"] and not row["age"].isdigit():
                raise ValidationError(f"customers row {row_number} has an invalid age")
            chunk.append({field: row[field] for field in fields})
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def validate_reference_file(path: Path, dataset: str, chunk_size: int) -> int:
    return sum(len(chunk) for chunk in iter_reference_chunks(path, dataset, chunk_size))
