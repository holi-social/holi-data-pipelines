FROM prefecthq/prefect:3.1.4-python3.12
COPY requirements.txt /opt/prefect/holi-data-pipelines/requirements.txt
COPY typesense /opt/prefect/holi-data-pipelines/typesense
RUN python -m pip install --no-cache-dir -r /opt/prefect/holi-data-pipelines/requirements.txt
COPY src /opt/prefect/holi-data-pipelines/src
WORKDIR /opt/prefect/holi-data-pipelines/

