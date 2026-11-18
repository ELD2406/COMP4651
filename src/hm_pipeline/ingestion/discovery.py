from pathlib import Path

from hm_pipeline.common.exceptions import ConfigurationError

SOURCE_FILES = {
    "transactions": "transactions_train.csv",
    "customers": "customers.csv",
    "articles": "articles.csv",
}


def discover_source(dataset_root: Path, name: str, configured_path: Path | None = None) -> Path:
    try:
        filename = SOURCE_FILES[name]
    except KeyError as exc:
        raise ConfigurationError(f"Unknown dataset source: {name}") from exc
    path = configured_path if configured_path is not None else dataset_root / filename
    if not path.is_absolute():
        path = dataset_root / path
    if not path.is_file():
        raise ConfigurationError(f"Missing {name} source file: {path}")
    return path
