FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system --gid 10001 ingest \
    && useradd --system --uid 10001 --gid ingest --create-home --home-dir /home/ingest ingest \
    && install -d -o ingest -g ingest /checkpoints /raw-data

COPY pyproject.toml ./
COPY src ./src
COPY tests ./tests

ARG INSTALL_S3=false
RUN if [ "$INSTALL_S3" = "true" ]; then \
        python -m pip install --no-cache-dir ".[kafka,s3]"; \
    else \
        python -m pip install --no-cache-dir ".[kafka]"; \
    fi

USER ingest:ingest

ENTRYPOINT ["python", "-m", "hm_pipeline.cli", "--config", "/app/config.docker.toml"]
