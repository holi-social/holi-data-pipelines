# Ingest BQ data into TS

## How to develop / run locally

Fill in the values in `.envrc.local` with staging values, run locally:

## How to run Typesense locally in a docker container

```
export TYPESENSE_API_KEY=xyz
    
mkdir "$(pwd)"/typesense-data

docker run -p 8108:8108 \
            -v"$(pwd)"/typesense-data:/data typesense/typesense:27.1 \
            --data-dir /data \
            --api-key=$TYPESENSE_API_KEY \
            --enable-cors
```

## How to run the pipeline on Prefect

```
pip install -U prefect

prefect server start

prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

python src/pipelines/bq_to_typesense/pipeline.py
```