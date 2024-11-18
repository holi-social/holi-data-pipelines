terraform {
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.12"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.12"
    }
    prefect = {
      source = "prefecthq/prefect"
    }
    null = {
      source  = "hashicorp/null"
      version = ">= 3.2.2"
    }
  }
  backend "gcs" {
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

data "terraform_remote_state" "holi_data_pipeline_common_state" {
  backend = "gcs"
  config = {
    bucket = "holi-shared-terraform-state"
    prefix = "data-pipeline-common"
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

provider "prefect" {
  endpoint = "http://${local.prefect_internal_domain}:${local.prefect_internal_port}"
}
