# holi Data Pipelines

This project contains code and infrastructure definitions for all data pipelines and real-time (pub/sub-based) cloud
function(s).

<!-- TOC -->
* [holi Data Pipelines](#holi-data-pipelines)
  * [Local Development](#local-development)
    * [Setup](#setup)
      * [Troubleshooting](#troubleshooting)
    * [Testing](#testing)
    * [Running pipelines](#running-pipelines)
  * [Remote Development (running developed pipelines/flows in GCP)](#remote-development-running-developed-pipelinesflows-in-gcp)
  * [Infrastructure](#infrastructure)
  * [Running](#running)
  * [Typesense](#typesense)
<!-- TOC -->

The repo uses `uv` (<https://docs.astral.sh/uv/>) as package and project manager. To use it, please install `uv` as
described on their website. The repo uses `uv`s workspaces concept to realize a monorepo structure (one repo with
multiple, potentially interdependent projects):

* `pubsub_typesense_updater` contains code for a Google Cloud Function that consumes domain event messages from Google
   Cloud Pub/Sub and updates the Typesense index accordingly
* `src` contains data pipelines to be run with `Prefect` (<https://www.prefect.io>).
* `shared` contains shared code `pubsub_typesense_updater` and `src`.

## Local Development

### Setup

You need some tools installed: `uv`, `direnv`. See holi-meta/README.md for more information.

The project uses uv workspaces. To sync and install dependencies for all workspaces you can run:

```shell
uv sync --all-packages
```

Since [prefect](#prefect) is not publicly available, but only via the GCP VPC, to execute terraform, we can
make use of an SSH tunnel by following these steps (work for staging and production):

* Add the following line to your `etc/hosts`:
  `127.0.0.1 prefect-server-staging.holi.internal prefect-server-production.holi.internal`

Make sure you have this project checked out directly in a checkout of holi-meta. Execute

```shell
direnv allow
````

This should lead to no errors and establish the `ssh` tunnels. If you run into errors, see the troubleshooting  section.

Let's run an example pipeline:

```shell
uv run --package src src/pipelines/example/pipeline.py
```

If the pipeline runs successfully, you're all set up.

#### Troubleshooting

The direnv setup tries to set up a full dev environment. If it fails, here's a description of the necessary steps so you
can debug the failures:

* `PREFECT_API_URL` env var needs to be set to the staging or production instance (in `.envrc.local`, initialized
  with defaults from `.envrc.local.template`)
  * OR you need to have the env var unset and provide all configuration for your flow via environment variables
* If you have `PREFECT_API_URL` set, you need an ssh tunnel to the prefect instance referenced by it.
  This normally is done for you via defaults in `.envrc.local`. For staging, you can do this manually via
  `gcloud compute ssh --zone "europe-north1-a" prefect-server-staging-green --tunnel-through-iap --project "holi-shared" -- -L8421:localhost:8421`.
  For Production, adapt the hostname and the port (see `.envrc.local.template` again).
* The default tunnel only has a lifetime of a day. If you suspect it's gone, try `direnv reload`.

### Testing

For data pipelines, tests are executed against the staging environment if they contain any Prefect-related code. If
they're just plain old unit test code not using Prefect-related constructs they are simply ran locally.

```shell
# run tests for pubsub_typesense_updater
uv run --package pubsub_typesense_updater pytest pubsub_typesense_updater

# run tests for shared
uv run --package shared pytest shared

# run tests for pipelines
uv run --package src pytest src
```

### Running pipelines

```shell
uv run --package src path/to/pipeline.py
```

## Remote Development (running developed pipelines/flows in GCP)

After developing a pipeline locally, you should test whether it works in the GCP environment, on the staging instance of
prefect. To do that, follow these steps:

* Make sure you developed a flow that works when running it locally.
* Add a deployment for the flow via the `src/pipelines/deployments.py` file. You can keep it simple and copy the
  deployment of the example pipeline ("hello universe"). This will allow you to run the flow in the prefect environment.
* Create a branch and merge request for your flow and deployment, so it can be code reviewed later.
* Merge the MR to main, so the staging prefect environment gets deployed with the new code.
* Wait for the GitLab CI pipeline to successfully
  finish: <https://gitlab.holi.team/app/holi-data-pipelines/-/pipelines?ref=main>
* Run the flow (via the deployment) in the prefect UI.
  Visit <http://prefect-server-staging.holi.internal:8421/deployments> and find your flow. Press the "three-buttons-menu"
  at the right and select "Quick run" for a run without specifying any parameters, or select "Custom run" to specify
  parameters for the run.
* Go to the run details (either by clicking "View run" in the notification that appears or by navigating to
  <http://prefect-server-staging.holi.internal:8421/runs> and to the run detail page). You can monitor it's progress
  and status there.
* If you need to inspect further, once a flow has finished, prefect provides a link to GCP logs at the end of the run
  details page. This link might only show up after a reload of the page.

## Infrastructure

This project defines data pipeline infrastructure in the `terraform` folder and it's subfolders.

```shell
pyenv install $(cat .python-version)
```

Install `terraform` via <https://github.com/tfutils/tfenv> .

Install `direnv` via <https://direnv.net/docs/installation.html> .

Allow direnv for the directory:

```shell
direnv allow
```

Initialize terraform in the corresponding repositories `terraform/common` and `terraform/environments`:

```shell
terraform init
```

## Running

Now, you can run terraform (in the corresponding repositories `terraform/common` and `terraform/environments`) via

```shell
terraform plan
terraform apply
```

## Typesense

See `TYPESENSE.md`.

## Local execution

Set up the environment variables missing from `.envrc.local.template`.

For LLM usage login with `gcloud`:

```sh
gcloud auth application-default login
```

To start the a tunnel for redis cache (IP might change, checkout GCloud Memory Store Connections Primary Endpoint):

```sh
gcloud compute ssh --zone europe-north1-a nginx-proxy-old-domains-and-okuna-blue --tunnel-through-iap --project holi-shared -- -L6379:10.104.0.11:6379
```
