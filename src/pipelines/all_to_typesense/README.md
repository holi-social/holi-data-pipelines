# all_to_typesense - Ingestion of all search data from scratch

## (Current) process to re-ingest from scratch

Keeping the Typesense index up-to-date with data from various sources (currently: okuna, volunteering) and for different
entities is a job whose responsibility is split between

* initial ingest (this job)
* subsequent "real-time" / run-time updates (via pub/sub)

When, for some reason, an ingestion from scratch is needed, the currently correct approach is to:

1. Stop the pub/sub consumer that updates Typesense documents (`holi-search_integration`). This can e.g. be done by
   deleting the `holi-search_integration` cloud function manually from https://console.cloud.google.com/functions/.
   Be aware that this might raise alerts in monitoring due to unacknowledged messages piling up in the subscription.
   This is exactly what we want, so we can process them after initial ingestion. 
2. Run this job here for initial ingestion using the Prefect UI.
3. Redeploy `holi-search_integration` by triggering the CI/CD pipeline for the specific environment. Ensure the cloud 
   function has been deployed again and works correctly.