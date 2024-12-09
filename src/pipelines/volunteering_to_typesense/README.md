# Ingest BQ data into TS

## How to run Typesense locally in a docker container

```
export $TYPESENSE_ADMIN_API_KEY=xyz
    
mkdir "$(pwd)"/typesense-data

docker run -p 8108:8108 \
            -v"$(pwd)"/typesense-data:/data typesense/typesense:27.1 \
            --data-dir /data \
            --api-key=$TYPESENSE_ADMIN_API_KEY \
            --enable-cors
```