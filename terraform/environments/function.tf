# see https://cloud.google.com/functions/docs/tutorials/terraform-pubsub

resource "random_id" "main" {
  byte_length = 2
  prefix      = "pubsub-typesense-updater-${local.environment_name}-"
}

resource "google_storage_bucket_object" "function_source" {
  name   = "${random_id.main.hex}/pubsub_typesense_updater.zip"
  bucket = data.terraform_remote_state.holi_infra_state.outputs.gcf_sources_upload_bucket_name
  source = "pubsub_typesense_updater/.build/pubsub_typesense_updater.zip"
}

resource "google_cloudfunctions2_function" "pubsub_typesense_updater" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = random_id.main.hex
  location     = local.default_region
  kms_key_name = data.terraform_remote_state.holi_infra_state.outputs.artifact_registry_cloudfunctions_key_id

  build_config {
    runtime           = "python312"
    entry_point       = "process_message"
    docker_repository = data.terraform_remote_state.holi_infra_state.outputs.cloudfunctions_repository_id

    source {
      storage_source {
        bucket = data.terraform_remote_state.holi_infra_state.outputs.gcf_sources_upload_bucket_name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count               = 1
    min_instance_count               = 0
    available_memory                 = "200Mi"
    timeout_seconds                  = 120
    available_cpu                    = 1
    max_instance_request_concurrency = 5

    environment_variables = {
      ENVIRONMENT               = local.environment_name
      TYPESENSE_HOST            = "t9vemwkibxajcorqp-1.a1.typesense.net"
      TYPESENSE_PORT            = "443"
      TYPESENSE_PROTOCOL        = "https"
      TYPESENSE_COLLECTION_NAME = local.environment_name == "production" ? "holi_search_production" : "holi_search_staging"
    }

    vpc_connector                  = data.terraform_remote_state.holi_infra_state.outputs.vpc_access_connector_name
    vpc_connector_egress_settings  = "PRIVATE_RANGES_ONLY"
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true
    service_account_email          = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.function_service_account

    secret_environment_variables {
      key        = "TYPESENSE_ADMIN_API_KEY"
      project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      secret     = local.environment_name == "production" ? "TYPESENSE_CLOUD_ADMIN_API_KEY_PRODUCTION" : "TYPESENSE_CLOUD_ADMIN_API_KEY_STAGING"
      version    = "latest"
    }
  }

  labels = {
    "environment_type" = local.environment_type
    "holi_service"     = "pubsub-typesense-updater"
  }

  # taken from https://github.com/hashicorp/terraform-provider-google/issues/1938#issuecomment-1229042663
  lifecycle {
    replace_triggered_by = [
      google_storage_bucket_object.function_source
    ]
  }
}
