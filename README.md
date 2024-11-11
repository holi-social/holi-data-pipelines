# holi Data Pipeline

This project contains infrastructure and code for the data pipeline.

## Infrastructure

This project defines data pipeline infrastructure.

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