terraform {
  # allow the lowest common version across all projects, so that the current CI docker image version suits all projects
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.13.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "7.13.0"
    }
    prefect = {
      source  = "prefecthq/prefect"
      version = "2.90.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "3.2.4"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "3.6.2"
    }
  }
  backend "gcs" {
    bucket = "holi-shared-terraform-state"
    prefix = "data-pipeline-environments"
  }
}

provider "docker" {
  #host = "unix:///var/run/docker.sock"
  registry_auth {
    address = "europe-north1-docker.pkg.dev"
  }
}

data "terraform_remote_state" "holi_infra_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "infra"
  }
}

data "terraform_remote_state" "holi_infra_monitoring_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "infra-monitoring"
  }
}

data "terraform_remote_state" "holi_data_pipeline_common_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "data-pipeline-common"
  }
}

data "terraform_remote_state" "okuna_common_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "okuna-common"
  }
}

data "terraform_remote_state" "okuna_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "okuna-environments"
  }
  workspace = terraform.workspace
}

# provider google including beta features
provider "google" {
  project = "holi-shared" # string, because referencing google_project.holi_shared would be a cycle
  region  = local.default_region
}

provider "google-beta" {
  project = "holi-shared" # string, because referencing google_project.holi_shared would be a cycle
  region  = local.default_region
}

provider "prefect" {
  endpoint = "http://${local.prefect_internal_domain}:${local.prefect_internal_port}"
}
