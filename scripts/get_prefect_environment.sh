#!/usr/bin/env sh

# used in prefect.yaml

echo $PREFECT_API_URL |sed 's/^.*prefect-server-\([^.]*\).holi.internal.*$/\1/g'
