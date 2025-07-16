resource "google_project_iam_member" "external_data_pubsub_publisher" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  member  = "serviceAccount:${data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email}"
  role    = "roles/pubsub.publisher"
}

resource "google_kms_crypto_key_iam_member" "external_data_pubsub_publisher_encrypter" {
  crypto_key_id = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key
  role          = "roles/cloudkms.cryptoKeyEncrypterEncrypter"
  member        = "serviceAccount:${data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email}"
}

resource "google_kms_crypto_key_iam_member" "external_data_pubsub_publisher_decrypter" {
  crypto_key_id = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email}"
}

resource "google_pubsub_topic" "external_data_staging" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "external-data-staging"
  kms_key_name = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key

  message_storage_policy {
    allowed_persistence_regions = [local.default_region]
  }
}


resource "google_pubsub_topic" "external_data_production" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "external-data-production"
  kms_key_name = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key

  message_storage_policy {
    allowed_persistence_regions = [local.default_region]
  }
}

output "google_pubsub_topic_external_data_staging" {
  value = google_pubsub_topic.external_data_staging.id
}

output "google_pubsub_topic_external_data_staging_name" {
  value = google_pubsub_topic.external_data_staging.name
}

output "google_pubsub_topic_external_data_production" {
  value = google_pubsub_topic.external_data_production.id
}

output "google_pubsub_topic_external_data_production_name" {
  value = google_pubsub_topic.external_data_production.name
}


