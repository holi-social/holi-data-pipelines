resource "google_pubsub_topic" "external_data_staging" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "external-data-staging"
  kms_key_name = google_kms_crypto_key.data_pipeline.id

  message_storage_policy {
    allowed_persistence_regions = [local.default_region]
  }
}


resource "google_pubsub_topic" "external_data_production" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "external-data-production"
  kms_key_name = google_kms_crypto_key.data_pipeline.id

  message_storage_policy {
    allowed_persistence_regions = [local.default_region]
  }
}

resource "google_pubsub_topic" "donations_staging" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "donations-staging"
  kms_key_name = google_kms_crypto_key.data_pipeline.id

  message_storage_policy {
    allowed_persistence_regions = [local.default_region]
  }
}


resource "google_pubsub_topic" "donations_production" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name         = "donations-production"
  kms_key_name = google_kms_crypto_key.data_pipeline.id

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

output "google_pubsub_topic_donations_staging_name" {
  value = google_pubsub_topic.donations_staging.name
}

output "google_pubsub_topic_donations_production_name" {
  value = google_pubsub_topic.donations_production.name
}
