FROM europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3.1.15-python3.12
COPY requirements.txt /opt/prefect/holi-data-pipelines/requirements.txt
RUN python -m pip install --no-cache-dir -r /opt/prefect/holi-data-pipelines/requirements.txt
COPY src /opt/prefect/holi-data-pipelines/src
WORKDIR /opt/prefect/holi-data-pipelines/

