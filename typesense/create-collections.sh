#!/bin/sh

curl "http://localhost:8108/collections" \
       -X POST \
       -H "Content-Type: application/json" \
       -H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_API_KEY}" \
       -d @all.json 
