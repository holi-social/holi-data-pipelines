# Environment based infrastructure for the Data Pipelines

This folder contains environment based infrastructure for the Data Pipelines.

## terraform from dev machine

Since (prefect)[#prefect] is nut publically available, but only via the GCP VPC, in order to execute terraform, we can make use of an SSH tunnel by following these steps (work for staging and production):

* Add the following line to your `etc/hosts`:
  `127.0.0.1 prefect-server-staging.holi.internal prefect-server-production.holi.internal`
* Before you execute any `terraform ...` command, execute, depending on environment (in a separate shell, and keep open), example for staging:
  `gcloud compute ssh --zone "europe-north1-a" prefect-server-staging-green --project "holi-shared" --tunnel-through-iap -- -L8420:localhost:8420`

## Prefect

### Server

In ´prefect-server.tf`, we define a VM that hosts the prefect server. It runs with the data-pipeline service account. 

First run, please execute:

`terraform apply -target=google_compute_instance.prefect_server_green -target=google_dns_record_set.prefect_server_holi_internal`

Afterwards, the server is available and you can provision the prefect specific resources with a simple

`terraform apply`

If you want to redeploy/re-setup the prefect server, please do

* `terraform destroy -target=google_compute_instance.prefect_server_green`
* `terraform apply -target=google_compute_instance.prefect_server_green -target=google_dns_record_set.prefect_server_holi_internal`
* `terraform apply -target=null_resource.prefect_server_available` (repeat once if this fails)
* you probably will need to execute `direnv reload` now in the project root for re-enabling the ssh tunnels
* `terraform apply`