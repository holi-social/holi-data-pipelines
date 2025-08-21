terraform {
  # allow the lowest common version across all projects, so that the current CI docker image version suits all projects
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.41.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "6.41.0"
    }
  }
  backend "gcs" {
    bucket = "holi-shared-terraform-state"
    prefix = "pubsub-typesense-updater-environments"
  }
}

data "terraform_remote_state" "pubsub_typesense_updater_common_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "pubsub-typesense-updater-common"
  }
}

data "terraform_remote_state" "data_pipelines_common_state" {
  backend   = "gcs"
  workspace = terraform.workspace
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "data-pipeline-common"
  }
}

data "terraform_remote_state" "data_pipelines_environment_state" {
  backend   = "gcs"
  workspace = terraform.workspace
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "data-pipeline-environments"
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

data "terraform_remote_state" "okuna_common_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "okuna-common"
  }
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
