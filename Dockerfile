FROM europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3.4.0-python3.13
COPY shared-requirements.txt /opt/prefect/holi-data-pipelines/shared-requirements.txt
COPY requirements.txt /opt/prefect/holi-data-pipelines/requirements.txt
RUN python -m pip install --no-cache-dir -r /opt/prefect/holi-data-pipelines/requirements.txt
COPY src /opt/prefect/holi-data-pipelines/src
COPY shared /opt/prefect/holi-data-pipelines/shared
WORKDIR /opt/prefect/holi-data-pipelines/

