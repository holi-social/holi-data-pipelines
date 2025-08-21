resource "random_id" "pubsub-typesense-updater-donations-sub-id" {
  byte_length = 2
  prefix      = "pubsub-typesense-updater-donations-sub-${local.environment_name}-"
}

# Dead letter topic for donations subscription
resource "google_pubsub_topic" "pubsub-typesense-updater-donations-dead-letter" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name    = "pubsub-typesense-updater-donations-dead-letter-${local.environment_name}"
}

# Grant the Cloud Pub/Sub service account subscriber role to the dead letter topic
resource "google_pubsub_topic_iam_member" "dead_letter_subscriber" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  topic   = google_pubsub_topic.pubsub-typesense-updater-donations-dead-letter.name
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:service-${data.terraform_remote_state.holi_infra_state.outputs.shared_project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "pubsub-typesense-updater-donations-sub" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name    = random_id.pubsub-typesense-updater-donations-sub-id.hex
  topic   = local.environment_name == "production" ? data.terraform_remote_state.data_pipelines_common_state.outputs.google_pubsub_topic_donations_production_name : data.terraform_remote_state.data_pipelines_common_state.outputs.google_pubsub_topic_donations_staging_name

  enable_message_ordering = true

  ack_deadline_seconds = 10 * 60 # 10 minutes, max. value TODO validate if required
  expiration_policy {
    ttl = "" # never expire
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pubsub-typesense-updater-donations-dead-letter.id
    max_delivery_attempts = 5
  }

  push_config {
    push_endpoint = google_cloudfunctions2_function.pubsub_typesense_updater.service_config[0].uri

    oidc_token {
      service_account_email = data.terraform_remote_state.pubsub_typesense_updater_common_state.outputs.function_service_account
    }
  }
}

resource "google_monitoring_alert_policy" "pubsub_unacked_messages_donations" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  display_name = "TOFU: PubSub Unacked Messages Alert (${local.environment_name} - donations) / subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-donations-sub.name}'"
  combiner     = "OR"
  conditions {
    display_name = "Unacked messages condition"
    condition_prometheus_query_language {
      query    = "rate(pubsub_googleapis_com:subscription_oldest_unacked_message_age{subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-donations-sub.name}'}[5m]) >= 1"
      duration = "300s"
    }
  }

  alert_strategy {
    auto_close = "300s"
  }

  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_slack_tofu_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_slack_tofu_data_staging_id]

  documentation {
    content   = "Alert triggered when Pub/Sub subscription(s) (subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-donations-sub.name}') have unacknowledged messages for more than 5 minutes. Please check the cloud function deployment and logs to see if the subscriber works correctly."
    mime_type = "text/markdown"
  }
}
