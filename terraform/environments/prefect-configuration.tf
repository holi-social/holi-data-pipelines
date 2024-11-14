resource "null_resource" "prefect_server_available" {
  provisioner "local-exec" {
    command = "scripts/wait-for-startup-script.sh -i ${google_compute_instance.prefect_server_green.name} -z ${google_compute_instance.prefect_server_green.zone}"
  }
}

resource "prefect_variable" "voltastics_raw_table" {
  name  = "volunteering_voltastics_raw_table"
  value = "holi-shared.datalake_production.volunteering_voltastics_raw"
}

resource "prefect_block" "application_default_credentials" {
  name      = "application-default-credentials"
  type_slug = "gcp-credentials"

  data = jsonencode({
    "project" = "holi-shared"
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
  depends_on = [null_resource.prefect_server_available, prefect_block.application_default_credentials]
}

resource "prefect_work_pool" "cloud_run" {
  name              = "cloud-run"
  type              = "cloud-run"
  paused            = false
  base_job_template = file("./cloud-run-work-pool-base-job-template.json")
  depends_on        = [null_resource.prefect_server_available]
}

resource "google_cloud_run_service" "work_pool_cloud_run_worker" {
  name     = "prefect-worker-${local.environment_name}"
  location = local.default_region # finland, low CO2 emissions
  # https://github.com/hashicorp/terraform-provider-google/issues/5898
  autogenerate_revision_name = true

  template {
    spec {
      service_account_name = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email
      containers {
        image = "europe-north1-docker.pkg.dev/holi-shared/docker-hub-remote/prefecthq/prefect:3-latest"
        args  = ["prefect", "worker", "start", "--install-policy", "always", "--with-healthcheck", "-p", "cloud-run", "-t", "cloud-run"]
        env {
          name  = "PREFECT_API_URL"
          value = "https://${local.prefect_dns_domain}/api"
        }
        resources {
          limits = {
            # cpu can only be scaled down to 1000m as long as container_concurrency is set to != 1 and cpu-throttling is set to false
            cpu    = "1000m"
            memory = "512Mi"
          }
          requests = {
            cpu    = "1000m"
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
      container_concurrency = 100 # 0 means thread safe, no restriction on max concurrency
    }

    metadata {
      annotations = {
        "run.googleapis.com/cpu-throttling"    = false
        "autoscaling.knative.dev/minScale"     = 1
        "run.googleapis.com/startup-cpu-boost" = "true"
      }
      # labels set on the revision level
      labels = {
        "environment_type" = local.environment_type
        "holi_service"     = "prefect-worker"
      }
    }
  }

  metadata {
    # labels set on the service level
    labels = {
      "environment_type" = local.environment_type
      "holi_service"     = "prefect-worker"
    }

  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [null_resource.prefect_server_available, prefect_work_pool.cloud_run]
}
