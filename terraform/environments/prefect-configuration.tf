# Prefect Block types of interest:
#  bigquery-warehouse
#  custom-webhook
#  database-credentials
#  date-time
#  discord-webhook
#  docker-host
#  docker-registry-credentials
#  gcp-credentials
#  gcpsecret
#  gcs-bucket
#  github-credentials
#  github-repository
#  gitlab-credentials
#  gitlab-repository
#  json
#  lambda-function
#  local-file-system
#  minio-credentials
#  opsgenie-webhook
#  pager-duty-webhook
#  remote-file-system
#  secret
#  shell-operation
#  slack-credentialsterraform/environments/datastream-monitoring.tf
#  slack-webhook
#  smb

resource "null_resource" "prefect_server_available" {
  provisioner "local-exec" {
    command = "scripts/wait-for-startup-script.sh -p ${local.default_project} -n ${google_compute_instance.prefect_server_green.name} -z ${google_compute_instance.prefect_server_green.zone} -t 600"
  }
}

# crawler / voltastics
resource "prefect_variable" "voltastics_crawler_table" {
  name = "voltastics_crawler_table"
  value = {
    project = local.default_project
    dataset = local.dataset_datalake
    table   = google_bigquery_table.volunteering_voltastics_raw.table_id
  }
}

resource "prefect_variable" "voltastics_api_url" {
  name  = "voltastics_api_url"
  value = "https://backend.voltastics.de/api/searchengagement"
}

resource "prefect_block" "voltastics_api_key" {
  name      = "voltastics-api-key"
  type_slug = "gcpsecret"

  data = jsonencode({
    "gcp_credentials" = {
      "project" = "holi-shared"
    }
    "secret_name"    = "VOLUNTEERING_VOLTASTICS_API_KEY"
    "secret_version" = "latest"
  })
  depends_on = [null_resource.prefect_server_available]
}

# crawler / betterplace
resource "prefect_variable" "betterplace_crawler_table" {
  name = "betterplace_crawler_table"
  value = {
    project = local.default_project
    dataset = local.dataset_datalake
    table   = google_bigquery_table.donations_betterplace_raw.table_id
  }
}

resource "prefect_variable" "betterplace_api_url" {
  name  = "betterplace_api_url"
  value = "https://api.betterplace.org/de/api_v4"
}

resource "prefect_variable" "bigquery_datalake" {
  name = "bigquery_datalake"
  value = {
    project = local.default_project
    dataset = local.dataset_datalake
  }
}

resource "prefect_variable" "bigquery_datapool" {
  name = "bigquery_datapool"
  value = {
    project = local.default_project
    dataset = local.dataset_datapool
  }
}

resource "prefect_variable" "volunteering_to_typesense_state" {
  name  = "volunteering_to_typesense_state"
  value = {}
}

resource "prefect_variable" "typesense_host" {
  name  = "typesense_host"
  value = "t9vemwkibxajcorqp-1.a1.typesense.net"
}

resource "prefect_variable" "typesense_port" {
  name  = "typesense_port"
  value = 443
}

resource "prefect_variable" "typesense_protocol" {
  name  = "typesense_protocol"
  value = "https"
}

resource "prefect_variable" "typesense_collection" {
  name  = "typesense_collection"
  value = local.environment == "production" ? "holi_search_production" : "holi_search_staging"
}

resource "prefect_variable" "prefect_environment" {
  name  = "prefect_environment"
  value = local.environment
}

resource "prefect_variable" "bigquery_okuna_dataset" {
  name  = "bigquery_okuna_dataset"
  value = local.environment == "production" ? "okuna_production_public" : "okuna_staging_public"
}

# create one: create the GCP secret in the ui console
# create a block that references it.
resource "prefect_block" "typesense_admin_api_key" {
  name      = "typesense-admin-api-key"
  type_slug = "gcpsecret"

  data = jsonencode({
    "gcp_credentials" = {
      "project" = "holi-shared"
    }
    "secret_name"    = local.environment == "production" ? "TYPESENSE_CLOUD_ADMIN_API_KEY_PRODUCTION" : "TYPESENSE_CLOUD_ADMIN_API_KEY_STAGING"
    "secret_version" = "latest"
  })
  depends_on = [null_resource.prefect_server_available]
}

resource "prefect_block" "bigquery_warehouse" {
  name      = "bigquery-warehouse"
  type_slug = "bigquery-warehouse"

  data = jsonencode({
    "gcp_credentials" = {
      "project" = "holi-shared"
    }
  })
  depends_on = [null_resource.prefect_server_available]
}

resource "prefect_block" "rocketchat_alert" {
  name      = "rocketchat-alert"
  type_slug = "custom-webhook"

  data = jsonencode({
    "name" : "Prefect Alert", # Shows up as sender of the alert in Rocket.Chat
    "method" : "POST",
    "url" : "https://projectholi.rocket.chat/hooks/6751e64714a4b1e7ecdb0eb8/KDnELf9iki8Emm8Gvwh46TcTfSTXxiWaZ2zuxtmChYRdnGs4?channel=${local.environment == "production" ? "66fd2e1cfd9d814af0fb30d0" : "66fd2e46fd9d814af0fb30d3"}",
    "json_data" : {
      "subject" : "{{subject}}", "name" : "{{name}}", "body" : "{{body}}",
      }, "headers" : {
      "Content-Type" : "application/json"
    }
  })
  depends_on = [null_resource.prefect_server_available]
}

resource "prefect_work_pool" "cloud_run" {
  name   = "cloud-run-v2"
  type   = "cloud-run-v2"
  paused = false
  base_job_template = templatefile("./cloud-run-work-pool-base-job-template.json", {
    name_prefix = "match-${substr(local.environment_name, 0, 1)}"
  })
  depends_on = [null_resource.prefect_server_available]
}

resource "docker_image" "prefect" {
  name = "europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3.2.11-python3.12"
}

resource "google_cloud_run_v2_service" "work_pool_cloud_run_worker" {
  name                = "prefect-worker-${local.environment_name}"
  location            = local.default_region # finland, low CO2 emissions
  deletion_protection = false

  template {
    service_account = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email
    containers {
      image = docker_image.prefect.name
      args = [
        "prefect", "worker", "start", "--install-policy", "never", "--with-healthcheck", "-p",
        prefect_work_pool.cloud_run.name, "-t", prefect_work_pool.cloud_run.type
      ]
      env {
        name  = "PREFECT_API_URL"
        value = "http://${local.prefect_internal_domain}:${local.prefect_internal_port}/api"
      }
      # setting the install-policy flag of the prefect worker command above to always leads to an upgrade of prefect to the latest version on service start. This breaks communication between the server and the worker service.
      # In order to set the flag to never (or any other value), we need to pre-install prefect-gcp[all_extras].
      # See https://linen.prefect.io/t/23212478/when-i-use-a-prefect-worker-gcp-cloud-run-v2-worker-to-be-pr for inspiration to this fix.
      env {
        name  = "EXTRA_PIP_PACKAGES"
        value = "prefect-gcp[all_extras]==0.6.2"
      }
      resources {
        cpu_idle          = false
        startup_cpu_boost = true
        limits = {
          cpu    = "1"
          memory = "1024Mi"
        }
      }
      startup_probe {
        failure_threshold = 10
        http_get {
          path = "/health"
        }
      }
      liveness_probe {
        http_get {
          path = "/health"
        }
      }
    }
    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network = data.terraform_remote_state.holi_infra_state.outputs.google_compute_network_default_name
      }
    }
    max_instance_request_concurrency = 100 # 0 means thread safe, no restriction on max concurrency
    labels = {
      "environment_type" = local.environment_type
      "holi_service"     = "prefect-worker"
    }
    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }
  }

  scaling {
    min_instance_count = 1
  }

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [prefect_work_pool.cloud_run]
}
