resource "prefect_work_pool" "cloud_run" {
  name              = "cloud-run"
  type              = "cloud-run"
  paused            = false
  base_job_template = file("./cloud-run-work-pool-base-job-template.json")
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
        # resources {
        #   limits = {
        #     # cpu can only be scaled down to 1000m as long as container_concurrency is set to != 1
        #     cpu    = local.environment == "production" ? "1000m" : "1000m"
        #     memory = local.environment == "production" ? "1024Mi" : "1024Mi"
        #   }
        #   requests = {
        #     cpu    = local.environment == "production" ? "1000m" : "1000m"
        #     memory = local.environment == "production" ? "1024Mi" : "1024Mi"
        #   }
        # }
        # startup_probe {
        #   initial_delay_seconds = 30
        #   period_seconds        = 5
        #   timeout_seconds       = 5
        #   failure_threshold     = 10
        #   http_get {
        #     path = "/"
        #   }
        # }
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
}
