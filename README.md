# holi Data Pipelines

This project contains code and infrastructure definitions for all data pipelines.

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