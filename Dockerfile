FROM europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3.2.11-python3.12 AS builder

# ======= Installation of uv following https://docs.astral.sh/uv/guides/integration/docker/#installing-uv ==============
# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
# Download the latest installer
ADD https://astral.sh/uv/0.7.2/install.sh /uv-installer.sh
# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh
# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"
# ======================================================================================================================

FROM builder AS app
WORKDIR /opt/prefect/holi-data-pipelines/
COPY uv.lock pyproject.toml /opt/prefect/holi-data-pipelines/
COPY shared /opt/prefect/holi-data-pipelines/shared
COPY src /opt/prefect/holi-data-pipelines/src
ENV UV_LINK_MODE=copy
RUN uv sync --compile-bytecode --locked --package src
ENV PATH="/opt/prefect/holi-data-pipelines/.venv/bin:$PATH"

