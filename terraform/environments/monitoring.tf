# does not work via terraform yet - it seems: https://cloud.google.com/monitoring/uptime-checks/private-checks
#resource "google_monitoring_uptime_check_config" "prefect_server" {
#}

resource "google_monitoring_alert_policy" "prefect_disk_utilization" {
  display_name          = "MATCHING: Prefect disk utilization (${local.environment_name})"
  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_staging_id]
  severity              = "WARNING"
  alert_strategy {
    auto_close = "86400s" # 1 day
  }
  combiner = "OR"
  conditions {
    display_name = "MATCHING: Prefect disk utilization (${local.environment_name})"
    condition_threshold {
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter = templatefile("./monitoring_prefect_disk_utilization.tftpl", {
        instance_id = google_compute_instance.prefect_server_green.instance_id
      })
      threshold_value = 95

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
      trigger {
        count   = 1
        percent = 0
      }
    }
  }
  documentation {
    content   = "MATCHING: Prefect disk utilization exceeded (${local.environment_name})"
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "prefect_cpu_utilization" {
  display_name          = "MATCHING: Prefect CPU utilization (${local.environment_name})"
  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_staging_id]
  severity              = "WARNING"
  alert_strategy {
    auto_close = "86400s" # 1 day
  }
  combiner = "OR"
  conditions {
    display_name = "MATCHING: Prefect CPU utilization (${local.environment_name})"
    condition_threshold {
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter = templatefile("./monitoring_prefect_cpu_utilization.tftpl", {
        instance_id = google_compute_instance.prefect_server_green.instance_id
      })
      threshold_value = 0.8

      aggregations {
        alignment_period   = "600s" # 300s is too short, alert triggers on setup of VM
        per_series_aligner = "ALIGN_MEAN"
      }
      trigger {
        count   = 1
        percent = 0
      }

    }
  }
  documentation {
    content   = "MATCHING: Prefect CPU utilization exceeded (${local.environment_name})"
    mime_type = "text/markdown"
  }
}





resource "google_monitoring_alert_policy" "prefect_memory_utilization" {
  display_name          = "MATCHING: Prefect Memory utilization (${local.environment_name})"
  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_staging_id]
  severity              = "WARNING"
  alert_strategy {
    auto_close = "86400s" # 1 day
  }
  combiner = "OR"
  conditions {
    display_name = "MATCHING: Prefect Memory utilization (${local.environment_name})"
    condition_threshold {
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter = templatefile("./monitoring_prefect_memory_utilization.tftpl", {
        instance_id = google_compute_instance.prefect_server_green.instance_id
      })
      threshold_value = 90

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
      trigger {
        count   = 1
        percent = 0
      }

    }
  }
  documentation {
    content   = "MATCHING: Prefect Memory utilization exceeded (${local.environment_name})"
    mime_type = "text/markdown"
  }
}




resource "google_monitoring_alert_policy" "prefect_system_log_errors" {
  display_name          = "MATCHING: Prefect system log errors (${local.environment_name})"
  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_staging_id]
  severity              = "WARNING"
  alert_strategy {
    auto_close = "86400s" # 1 day
    notification_rate_limit {
      period = "3600s"
    }
  }
  combiner = "OR"
  conditions {
    display_name = "MATCHING: Prefect system log errors (${local.environment_name})"
    condition_matched_log {
      filter = templatefile("./monitoring_prefect_system_log_errors.tftpl", {
        instance_id = google_compute_instance.prefect_server_green.instance_id
      })
    }
  }
  documentation {
    content   = "MATCHING: Prefect system log errors (${local.environment_name})"
    mime_type = "text/markdown"
  }
}




