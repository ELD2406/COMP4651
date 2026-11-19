# H&M Data Engineering Pipeline

## Progress

Stage 1 Python ingestion is implemented. It streams and validates transaction data, replays a configured date window to Kafka with customer-keyed messages and restart checkpoints, and validates customer and article snapshots for raw local or S3 storage. Configuration supports TOML and environment variables, with independent CLI commands for each workflow.

Unit tests cover validation, replay, checkpoints, Kafka publishing behavior, and storage backends.

The H&M dataset is excluded from Git. Place the extracted files in `h-and-m-personalized-fashion-recommendations/`.
