#!/usr/bin/env sh

# used in prefect.yaml

echo -n europe-north1-docker.pkg.dev/holi-shared/docker/data-pipeline-job-
echo -n $PREFECT_API_URL |sed 's/^.*prefect-server-\([^.]*\).holi.internal.*$/\1/g'
echo :latest
