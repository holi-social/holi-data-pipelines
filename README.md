# holi Data Pipelines

This project contains code and infrastructure definitions for all data pipelines.

<!-- TOC -->

* [holi Data Pipelines](#holi-data-pipelines)
    * [Dev Quickstart](#dev-quickstart)
        * [Troubleshooting](#troubleshooting)
    * [Infrastructure](#infrastructure)
    * [Setup](#setup)
    * [Running](#running)
    * [CI/CD](#cicd)
    * [Infrastructure: Typesense Cloud Setup](#infrastructure-typesense-cloud-setup)
        * [Current Cluster Hardware Setup (as of 02.12.2024)](#current-cluster-hardware-setup-as-of-02122024)
        * [Security](#security)

<!-- TOC -->

## Dev Quickstart

You need some tools installed: pyenv, direnv. See holi-meta/README.md for more information.

Make sure you have this project checked out directly in a checkout of holi-meta. Executing `direnv allow` and
`direnv reload` should lead to no errors. If you run into errors, see the troubleshooting section.

Let's run an example pipeline:

`python3 src/pipelines/hellp-world/pipeline.py`

If the pipeline runs successfully, you're all set up.

### Troubleshooting

The direnv setup tries to set up a full dev environment. If it fails, here's a description of the necessary steps so you
can debug the failures:

* `PREFECT_API_URL` env var needs to be set to the staging or production instance (in `.envrc.local`, initialized
  with defaults from `.envrc.local.template`)
    * OR you need to have the env var unset and provide all configuration for your flow via environment variables
* If you have `PREFECT_API_URL` set, you need an ssh tunnel to the prefect instance referenced by it.
  This normally is done for you via defaults in `.envrc.local`. For staging, you can do this manually via
  `gcloud compute ssh --zone "europe-north1-a" prefect-server-staging-green --project "holi-shared" -- -L8421:localhost:8421`.
  For Production, adapt the hostname and the port (see `.envrc.local.template` again).
* The default tunnel only has a lifetime of a day. If you suspect it's gone, try `direnv reload`.
*

## Infrastructure

This project defines data pipeline infrastructure in the `terraform` folder and it's subfolders.

## Setup

Install terraform via https://github.com/tfutils/tfenv .

Install direnv via https://direnv.net/docs/installation.html .

Allow direnv for the directory:

```
direnv allow
```

Initialize terraform in the corresponding repositories `terraform/common` and `terraform/environments`:

```
terraform init
```

## Running

Now, you can run terraform ( in the corresponding repositories `terraform/common` and `terraform/environments` ) via

```
terraform plan
terraform apply
```

etc.

## CI/CD

TODO

## Typesense Cloud

Typesense cloud setup is currently a manual process. The reason for this is that there are currently no mature Terraform
providers for provisioning Typesense cloud with.

## Learning / Good-to-Know

* You can only update / upsert by id column, not by any other. Therefore, it is advisable to set the id column to
  something well-known (easy) or to save the returned auto-generated ID (harder). For the first data type (volunteering)
  we use `volunteering_${ID}`.
* Ingesting a larger number of documents with semantic search (embeddings) turned on can easily saturate the CPU, at
  least when tried on low power hardware. This results in multi-second response times for search queries, because reads
  and writes seem to be processed sequentially on the server.
    * When bulk ingesting, the default is to have 40 writes as a batch before the next search request is served.
    * Batching can be done on client-side on server-side. The `batch_size` parameter on the Python client library
      controls client-side batching.
* Memory usage determined mostly by which fields and which type of fields are indexed (i.e. the schema).

### Current Cluster Hardware Setup (as of 02.12.2024)

We currently run one Typesense cluster containing collections for both staging and production environments. This should
be fine during initial feature development for global search. The setup lacks load isolation between environments and
might require to be updated into 2 clusters once load increases in either of staging or production envs.

![typesense-cluster-config_2024-12-02.png](readme_images/typesense-cluster-config_2024-12-02.png)

**Memory Requirements**
Memory is the most important sizing factor as Typesense holds the whole index in memory. The current index for
`staging_all` and `production_all` consumes about 2GB of memory each, resulting in a total of 4GBs for the cluster.

**CPUs**
2 vCPUs currently deliver low latency outside of ingestion times. During an active ingestion of documents, there's a
noticeable bigger latency but at this point of time it's considered good enough. Ingestion has been optimized using
client-side batching to result in lower latency (see pipeline code).

**All other settings**
The current setup is completely minimal to both save money and learn about the performance characteristics. During
feature development we'll most likely not need more hardware resources, this might change once we're in production.

### Security

Typesense allows scoping of API keys on two dimensions:

* collections an operation can be executed upon
* operations to be executed

Make sure to always constrain the API keys to the minimum required scopes in both dimensions and scope them to the
execution environment (staging / production).

Search-only API keys (those only allowing read operations) are no secrets and can be shared as part of client
applications.
