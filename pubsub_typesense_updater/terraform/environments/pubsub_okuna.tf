resource "google_pubsub_subscription" "pubsub-typesense-updater-okuna-sub" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name    = random_id.pubsub_typesense_updater.hex
  topic   = local.environment_name == "production" ? data.terraform_remote_state.okuna_common_state.outputs.google_pubsub_topic_okuna_production : data.terraform_remote_state.okuna_common_state.outputs.google_pubsub_topic_okuna_staging

  enable_message_ordering = true

  ack_deadline_seconds = 10 * 60 # 10 minutes, max. value TODO validate if required
  expiration_policy {
    ttl = "" # never expire
  }

  push_config {
    push_endpoint = google_cloudfunctions2_function.pubsub_typesense_updater.service_config[0].uri

    oidc_token {
      service_account_email = data.terraform_remote_state.pubsub_typesense_updater_common_state.outputs.function_service_account
    }
  }
}

resource "google_monitoring_alert_policy" "pubsub_unacked_messages_okuna" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  display_name = "TOFU: PubSub Unacked Messages Alert (${local.environment_name} - okuna) / subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-okuna-sub.name}'"
  combiner     = "OR"
  conditions {
    display_name = "Unacked messages condition"
    condition_prometheus_query_language {
      query    = "rate(pubsub_googleapis_com:subscription_oldest_unacked_message_age{subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-okuna-sub.name}'}[5m]) >= 1"
      duration = "300s"
    }
  }

  alert_strategy {
    auto_close = "300s"
  }

  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_slack_tofu_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_slack_tofu_data_staging_id]

  documentation {
    content   = "Alert triggered when Pub/Sub subscription(s) (subscription_id='${google_pubsub_subscription.pubsub-typesense-updater-okuna-sub.name}') have unacknowledged messages for more than 5 minutes. Please check the cloud function deployment and logs to see if the subscriber works correctly."
    mime_type = "text/markdown"
  }
}
