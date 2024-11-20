resource "null_resource" "prefect_server_available" {
  provisioner "local-exec" {
    command = "scripts/wait-for-startup-script.sh -i ${google_compute_instance.prefect_server_green.name} -z ${google_compute_instance.prefect_server_green.zone} -t 600"
  }
}

resource "prefect_variable" "volunteering_with_classification" {
  name  = "volunteering_with_classification"
  value = local.environment == "production" ? "holi-shared.datapool_production.volunteering_voltastics_with_classification_enriched" : "holi-shared.datapool_development.volunteering_voltastics_with_classification_enriched"
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

# TODO is this block needed? seems to work with direct embedding in depending resources
# resource "prefect_block" "application_default_credentials" {
#   name      = "application-default-credentials"
#   type_slug = "gcp-credentials"
#   data = jsonencode({
#     "project" = "holi-shared"
#   })
#   depends_on = [null_resource.prefect_server_available]
# }

resource "prefect_block" "typesense_admin_api_key" {
  name      = "typesense-admin-api-key"
  type_slug = "gcpsecret"

  data = jsonencode({
    "gcp_credentials" = {
      "project" = "holi-shared"
    }
    "secret_name"    = "TYPESENSE_ADMIN_API_KEY"
    "secret_version" = "latest"
  })
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

resource "prefect_work_pool" "cloud_run" {
  name              = "cloud-run"
  type              = "cloud-run"
  paused            = false
  base_job_template = file("./cloud-run-work-pool-base-job-template.json")
  depends_on        = [null_resource.prefect_server_available]
}

resource "google_cloud_run_v2_service" "work_pool_cloud_run_worker" {
  name     = "prefect-worker-${local.environment_name}"
  location = local.default_region # finland, low CO2 emissions

  template {
    service_account = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email
    containers {
      image = "europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3-latest"
      args  = ["prefect", "worker", "start", "--install-policy", "always", "--with-healthcheck", "-p", "cloud-run", "-t", "cloud-run"]
      env {
        name  = "PREFECT_API_URL"
        value = "http://${local.prefect_internal_domain}:${local.prefect_internal_port}/api"
      }
      resources {
        cpu_idle          = false
        startup_cpu_boost = true
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      startup_probe {
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
  }

  scaling {
    min_instance_count = 1
    #max_instance_count = 1 # TODO this should work, but does not
  }

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [null_resource.prefect_server_available]
}
