# H&M Data Engineering Pipeline

## Progress

Stage 1 Python ingestion is implemented. It streams and validates transaction data, replays a configured date window to Kafka with customer-keyed messages and restart checkpoints, and validates customer and article snapshots for raw local or S3 storage. Configuration supports TOML and environment variables, with independent CLI commands for each workflow.

Unit tests cover validation, replay, checkpoints, Kafka publishing behavior, and storage backends.

The H&M dataset is excluded from Git. Place the extracted files in `h-and-m-personalized-fashion-recommendations/`.

## Docker quick start

Docker Compose runs Kafka and the existing CLI in an on-demand container. With Docker Desktop running and the dataset extracted, run:

```powershell
docker compose build
docker compose up -d kafka
docker compose run --rm ingestion validate-transactions --limit 1000
```

See [Docker setup](docs/docker.md) for replay, storage, and S3 instructions, or [First Run setup](docs/first-run.md) for local Python setup.

## Analysis part
- Spark environment is configured (local mode, driven by `config.toml`).
- Data ingestion is done: the three H&M CSVs are loaded, cleaned, and validated in
  `notebooks/01_hm_pyspark_analysis.ipynb` for batch analysis.
- Completed analysis
  - GMV trend
    - ttm,month,quarter
    - yoy,qoq,mom
  - TOP K
    - group ranking
    - type ranking
    - prod ranking
    - type in group share
    - prod in type share 
