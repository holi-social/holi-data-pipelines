# see https://cloud.google.com/functions/docs/tutorials/terraform-pubsub

resource "random_id" "pubsub_typesense_updater" {
  byte_length = 2
  prefix      = "pubsub-typesense-updater-${local.environment_name}-"
}

resource "google_storage_bucket_object" "function_source" {
  name   = "${random_id.pubsub_typesense_updater.hex}/pubsub_typesense_updater.zip"
  bucket = data.terraform_remote_state.holi_infra_state.outputs.gcf_sources_upload_bucket_name
  source = "../../.build/pubsub_typesense_updater.zip"
}

resource "google_cloudfunctions2_function" "pubsub_typesense_updater" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = random_id.pubsub_typesense_updater.hex
  location     = local.default_region
  kms_key_name = data.terraform_remote_state.holi_infra_state.outputs.artifact_registry_cloudfunctions_key_id

  build_config {
    runtime           = "python313"
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
    max_instance_count               = 10
    min_instance_count               = 0
    available_memory                 = "2048Mi"
    timeout_seconds                  = 120
    available_cpu                    = 1
    max_instance_request_concurrency = 5

    environment_variables = {
      ENVIRONMENT                         = local.environment_name
      TYPESENSE_HOST                      = "t9vemwkibxajcorqp-1.a1.typesense.net"
      TYPESENSE_PORT                      = "443"
      TYPESENSE_PROTOCOL                  = "https"
      TYPESENSE_COLLECTION_NAME           = local.environment_name == "production" ? "holi_search_production" : "holi_search_staging"
      GCLOUD_PROJECT_ID                   = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      GCLOUD_LOCATION                     = local.default_region
      BIGQUERY_TABLE_NAME_CLASSIFICATIONS = data.terraform_remote_state.data_pipelines_environment_state.outputs.bigquery_table_name_classifications
      REDIS_HOST                          = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.redis_host_cmek_production : data.terraform_remote_state.holi_infra_state.outputs.redis_host_cmek_development
      REDIS_PORT                          = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.redis_port_cmek_production : data.terraform_remote_state.holi_infra_state.outputs.redis_port_cmek_development
      REDIS_DB                            = local.environment_name == "production" ? 14 : 6
      LANGFUSE_HOST                       = "https://cloud.langfuse.com"
      CACHE_DEBUG                         = "True"
      VERTEX_AI_PROJECT                   = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      VERTEX_AI_LOCATION                  = local.default_region
      VERTEX_AI_RETRIES                   = "2"
    }

    vpc_connector                  = data.terraform_remote_state.holi_infra_state.outputs.vpc_access_connector_name
    vpc_connector_egress_settings  = "PRIVATE_RANGES_ONLY"
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true
    service_account_email          = data.terraform_remote_state.pubsub_typesense_updater_common_state.outputs.function_service_account

    secret_environment_variables {
      key        = "TYPESENSE_ADMIN_API_KEY"
      project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      secret     = local.environment_name == "production" ? "TYPESENSE_CLOUD_ADMIN_API_KEY_PRODUCTION" : "TYPESENSE_CLOUD_ADMIN_API_KEY_STAGING"
      version    = "latest"
    }

    secret_environment_variables {
      key        = "REDIS_PASSWORD"
      project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      secret     = "REDIS_CMEK_PASSWORD"
      version    = "latest"
    }

    secret_environment_variables {
      key        = "LANGFUSE_SECRET_KEY"
      project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      secret     = "LANGFUSE_SECRET_KEY"
      version    = "latest"
    }

    secret_environment_variables {
      key        = "LANGFUSE_PUBLIC_KEY"
      project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
      secret     = "LANGFUSE_PUBLIC_KEY"
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
