FROM europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3.4.0-python3.13 AS builder
COPY --from=europe-north1-docker.pkg.dev/holi-shared/ghcr-io-remote/astral-sh/uv:latest /uv /uvx /bin/

FROM builder AS app
WORKDIR /opt/prefect/holi-data-pipelines/
COPY uv.lock pyproject.toml /opt/prefect/holi-data-pipelines/
COPY shared /opt/prefect/holi-data-pipelines/shared
COPY src /opt/prefect/holi-data-pipelines/src
ENV UV_LINK_MODE=copy
RUN uv sync --compile-bytecode --package src
ENV PATH="/opt/prefect/holi-data-pipelines/.venv/bin:$PATH"

