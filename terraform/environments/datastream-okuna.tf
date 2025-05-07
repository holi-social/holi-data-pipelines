locals {
  secret_postgresql_okuna_username      = local.environment_name == "production" ? "OKUNA_DB_USERNAME_PRODUCTION" : "OKUNA_DB_USERNAME_DEVELOPMENT"
  secret_postgresql_okuna_password      = local.environment_name == "production" ? "OKUNA_DB_PASSWORD_PRODUCTION" : "OKUNA_DB_PASSWORD_DEVELOPMENT"
  secret_postgresql_datastream_username = local.environment_name == "production" ? "POSTGRES_DATASTREAM_USERNAME_PRODUCTION" : "POSTGRES_DATASTREAM_USERNAME_DEVELOPMENT"
  secret_postgresql_datastream_password = local.environment_name == "production" ? "POSTGRES_DATASTREAM_PASSWORD_PRODUCTION" : "POSTGRES_DATASTREAM_PASSWORD_DEVELOPMENT"
  postgresql_database_name              = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.database_name_production : data.terraform_remote_state.holi_infra_state.outputs.database_name_development
  postgresql_database_ip_address        = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.database_ip_address_production : data.terraform_remote_state.holi_infra_state.outputs.database_ip_address_development
  # IMPORTANT: only add tables to the end of the list, otherwise it will run a full backfill for all tables
  datastream_okuna_tables                       = [
    "auth_group",
    "auth_permission",
    "auth_group_permissions",
    "django_content_type",
    "authtoken_token",
    "django_admin_log",
    "django_migrations",
    "django_session",
    "favorites_userfavorite",
    "feed_posts_feedpost",
    "feed_posts_feedpost_topics",
    "feed_posts_feedpostimage",
    "feed_posts_feedpostmention",
    "feed_posts_feedpostreaction",
    "openbook_auth_user",
    "openbook_appointments_appointment",
    "openbook_appointments_appointment_participants",
    "openbook_appointments_locationdetails",
    "openbook_appointments_videocalllink",
    "openbook_auth_user_groups",
    "openbook_auth_user_user_permissions",
    "openbook_auth_userblock",
    "openbook_auth_usernotificationssubscription",
    "openbook_auth_usernotificationssettings",
    "openbook_auth_userprofile_badges",
    "openbook_auth_userprofile",
    "openbook_circles_circle",
    "openbook_auth_userprofile_favorite_insights",
    "openbook_auth_userprofile_sdgs",
    "openbook_auth_userprofile_skills",
    "openbook_auth_userprofile_interests",
    "openbook_categories_category",
    "openbook_categories_category_communities",
    "openbook_circles_circle_posts",
    "openbook_circles_circle_connections",
    "openbook_common_appversion",
    "openbook_common_linkpreview",
    "openbook_common_language",
    "openbook_common_badge",
    "openbook_common_emojigroup",
    "openbook_common_emoji",
    "openbook_communities_collaborationtool",
    "openbook_common_proxyblacklisteddomain",
    "openbook_communities_community",
    "openbook_communities_communityinvite",
    "openbook_communities_communitymembershiprequest",
    "openbook_communities_community_banned_users",
    "openbook_communities_community_sdgs",
    "openbook_communities_communitymembership",
    "openbook_communities_communitylog",
    "openbook_communities_community_starrers",
    "openbook_communities_community_topics",
    "openbook_communities_communitynotificationssubscription",
    "openbook_communities_onboardingstep",
    "openbook_communities_task",
    "openbook_communities_communitypendinginvite",
    "openbook_communities_externallink",
    "openbook_communities_spacesrelationship",
    "openbook_connections_connection",
    "openbook_communities_task_skills",
    "openbook_devices_device",
    "openbook_follows_follow",
    "openbook_follows_followrequest",
    "openbook_hashtags_hashtag",
    "openbook_hashtags_hashtag_post_comments",
    "openbook_hashtags_hashtag_posts",
    "openbook_hashtags_hashtag_insights",
    "openbook_insights_insight",
    "openbook_insights_discussion",
    "openbook_insights_initiative",
    "openbook_insights_initiativereaction",
    "openbook_insights_insight_posts",
    "openbook_insights_insight_polls",
    "openbook_insights_quote",
    "openbook_insights_quotereaction",
    "openbook_insights_recommendation",
    "openbook_insights_recommendationreaction",
    "openbook_invitations_userinvite",
    "openbook_lists_list_follows",
    "openbook_lists_list",
    "openbook_moderation_moderatedobject",
    "openbook_moderation_moderatedobjectcategorychangedlog",
    "openbook_moderation_moderatedobjectdescriptionchangedlog",
    "openbook_moderation_moderationcategory",
    "openbook_moderation_moderationreport",
    "openbook_moderation_moderatedobjectstatuschangedlog",
    "openbook_moderation_moderatedobjectlog",
    "openbook_moderation_moderatedobjectverifiedchangedlog",
    "openbook_moderation_moderationpenalty",
    "openbook_notifications_communityadminprivilegesrevokednotif2441",
    "openbook_notifications_communityinvitedeclinednotification",
    "openbook_notifications_communityadminprivilegesgrantednotif875b",
    "openbook_notifications_communitymembershiprevokednotification",
    "openbook_notifications_communitymembershipapprovednotification",
    "openbook_notifications_communityinvitenotification",
    "openbook_notifications_communitynewpostnotification",
    "openbook_notifications_notification",
    "openbook_notifications_followrequestapprovednotification",
    "openbook_notifications_followrequestnotification",
    "openbook_notifications_communitymembershiprequestnotification",
    "openbook_notifications_postcommentreplynotification",
    "openbook_notifications_postcommentnotification",
    "openbook_notifications_postcommentreactionnotification",
    "openbook_notifications_connectionacceptednotification",
    "openbook_notifications_connectionconfirmednotification",
    "openbook_notifications_connectionrequestnotification",
    "openbook_notifications_follownotification",
    "openbook_posts_post",
    "openbook_posts_postcomment",
    "openbook_polls_poll",
    "openbook_notifications_postcommentusermentionnotification",
    "openbook_notifications_releasenotification",
    "openbook_notifications_usernewpostnotification",
    "openbook_notifications_postreactionnotification",
    "openbook_notifications_postusermentionnotification",
    "openbook_polls_choice",
    "openbook_polls_vote",
    "openbook_posts_post_links",
    "openbook_posts_postcommentreaction",
    "openbook_posts_postcommentusermention",
    "openbook_posts_postreaction",
    "openbook_posts_postusermention",
    "openbook_posts_postcomment_links",
    "openbook_posts_postcommentmute",
    "openbook_posts_postimage",
    "openbook_posts_postmedia",
    "openbook_posts_postmute",
    "openbook_terms_feedpostcategory",
    "openbook_terms_sdg",
    "openbook_terms_skill",
    "openbook_posts_postvideo",
    "openbook_posts_profilepostscommunityexclusion",
    "openbook_posts_toppostcommunityexclusion",
    "openbook_posts_toppost",
    "openbook_posts_trendingpost",
    "openbook_terms_topic",
    "scheduler_jobarg",
    "scheduler_repeatablejob",
    "scheduler_cronjob",
    "scheduler_jobkwarg",
    "scheduler_scheduledjob",
    "spatial_ref_sys",
    "video_encoding_format",
    "anon_stats_counter"
  ]
  datastream_okuna_publication_replication_name = "okuna_datastream"
}

data "google_secret_manager_secret_version_access" "postgres_datastream_username" {
  secret  = local.secret_postgresql_datastream_username
  project = local.default_project
}

data "google_secret_manager_secret_version_access" "postgres_datastream_password" {
  secret  = local.secret_postgresql_datastream_password
  project = local.default_project
}

resource "google_sql_user" "okuna_datastream" {
  name     = data.google_secret_manager_secret_version_access.postgres_datastream_username.secret_data
  instance = local.postgresql_database_name
  password = data.google_secret_manager_secret_version_access.postgres_datastream_password.secret_data
}

data "google_secret_manager_secret_version_access" "okuna_database_username" {
  secret  = local.secret_postgresql_okuna_username
  project = local.default_project
}

data "google_secret_manager_secret_version_access" "okuna_database_password" {
  secret  = local.secret_postgresql_okuna_password
  project = local.default_project
}

# https://cloud.google.com/datastream/docs/configure-cloudsql-psql
resource "null_resource" "database_setup" {
  # ensure that changes to the template and it's input vars triggers a redeployment of this null resource
  triggers = {
    template_hash        = filemd5("datastream-pg-db-setup.tftpl") # Will trigger on file changes
    database_server_name = local.postgresql_database_name
    okuna_database       = data.terraform_remote_state.okuna_state.outputs.database_name
    okuna_db_user        = data.google_secret_manager_secret_version_access.okuna_database_username.secret_data
    tables               = join(",", local.datastream_okuna_tables) # Convert list to string for triggering
    publication_name     = local.datastream_okuna_publication_replication_name
    datastream_db_user   = google_sql_user.okuna_datastream.name
  }

  provisioner "local-exec" {
    command = templatefile("datastream-pg-db-setup.tftpl", {
      database_server_name = local.postgresql_database_name
      okuna_database       = data.terraform_remote_state.okuna_state.outputs.database_name
      okuna_db_user        = data.google_secret_manager_secret_version_access.okuna_database_username.secret_data
      tables               = local.datastream_okuna_tables
      publication_name     = local.datastream_okuna_publication_replication_name
      datastream_db_user   = google_sql_user.okuna_datastream.name
    })
    environment = {
      PGPASSWORD = data.google_secret_manager_secret_version_access.okuna_database_password.secret_data
    }
  }

  depends_on = [google_sql_user.okuna_datastream]
}

# This VM is needed for routing traffic from datastream to PostGreSQL.
# Datastream is a Google Service that only can connect to our VPC, not directly to PostGreSQL.
# This machine is the target for Datastream and NAT forwards all packets to the DB.
resource "google_compute_instance" "datastream_nat_vm" {
  name = "datastream-nat-vm-${local.environment_name}"
  # default cheapest machine type
  machine_type = "f1-micro" # costs <5€/month
  # if f1-micro is not available at the time of recreation, try one of these (all below 20€/month):
  #machine_type = "e2-micro"
  #machine_type = "e2-small"
  #machine_type = "g1-small"
  zone = "${local.default_region}-a"
  tags = ["datastream-nat"]

  boot_disk {
    kms_key_self_link = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10 # uses less than 5GB after installation
    }
  }

  lifecycle {
    # ssh keys are provisioned on access, so we should not remove them on update
    ignore_changes = [metadata["ssh-keys"]]
  }

  network_interface {
    network = data.terraform_remote_state.holi_infra_state.outputs.google_compute_network_default_name
    # we want to only be reachable via the vpc - leaving out the access_config block here will avoid a public IP being assigned
  }

  metadata = {
    startup-script = templatefile("datastream-nat-vm-setup.tftpl", {
      ports_to_targets = {
        "5432" = "${local.postgresql_database_ip_address}"
      }
    })
  }
  labels = {
    purpose : "datastream"
    environment : local.environment_name
  }
}

resource "google_datastream_connection_profile" "okuna_source_connection_profile" {
  display_name          = "${title(local.environment_name)} Okuna source connection profile"
  location              = local.default_region
  connection_profile_id = "${local.environment_name}-okuna-source-profile"

  postgresql_profile {
    hostname = google_compute_instance.datastream_nat_vm.network_interface[0].network_ip
    username = google_sql_user.okuna_datastream.name
    password = google_sql_user.okuna_datastream.password
    database = data.terraform_remote_state.okuna_state.outputs.database_name
    port     = 5432
  }

  private_connectivity {
    private_connection = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.datastream_peering_id
  }

  depends_on = [google_compute_instance.datastream_nat_vm, null_resource.database_setup]
}

resource "google_datastream_connection_profile" "bigquery_destination_connection_profile" {
  display_name          = "${title(local.environment_name)} bigquery destination connection profile"
  location              = local.default_region
  connection_profile_id = "${local.environment_name}-bigquery-destination-profile"

  bigquery_profile {}
}

resource "google_datastream_stream" "okuna_to_bigquery" {
  stream_id     = "${local.environment_name}-okuna-to-bigquery"
  depends_on    = [null_resource.database_setup]
  desired_state = "RUNNING"
  location      = local.default_region
  display_name  = "Okuna to Bigquery (${local.environment_name})"
  source_config {
    source_connection_profile = google_datastream_connection_profile.okuna_source_connection_profile.id
    postgresql_source_config {
      max_concurrent_backfill_tasks = 12
      publication                   = local.datastream_okuna_publication_replication_name # same as publication name from manual steps in sql user
      replication_slot              = local.datastream_okuna_publication_replication_name # same as replication slot name from manual steps in sql user
      include_objects {
        postgresql_schemas {
          schema = "public"

          dynamic "postgresql_tables" {
            for_each = local.datastream_okuna_tables
            content {
              table = postgresql_tables.value
            }
          }
        }
      }
    }
  }

  destination_config {
    destination_connection_profile = google_datastream_connection_profile.bigquery_destination_connection_profile.id
    bigquery_destination_config {
      data_freshness = "900s"
      source_hierarchy_datasets {
        dataset_template {
          location          = local.default_region
          dataset_id_prefix = "okuna_${local.environment_name}_"
        }
      }
    }
  }

  backfill_all {}

  customer_managed_encryption_key = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key
}

# Log-based metric for schema changes in Okuna tables, used for alerting
resource "google_logging_metric" "datastream_schema_change" {
  name        = "datastream_postgres_schema_change_${local.environment_name}"
  description = "Count of schema change events in Okuna PostgreSQL tables replicated via Datastream"
  filter      = "resource.type=\"datastream.googleapis.com/Stream\" AND resource.labels.stream_id=\"${local.environment_name}-okuna-to-bigquery\" jsonPayload.event_code=\"BIGQUERY_WRITER_TABLE_SCHEMA_CHANGED\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "stream_id"
      value_type  = "STRING"
      description = "Datastream stream ID"
    }
    labels {
      key         = "database"
      value_type  = "STRING"
      description = "Database name"
    }
    labels {
      key         = "table"
      value_type  = "STRING"
      description = "Table name"
    }
  }

  label_extractors = {
    "stream_id" = "EXTRACT(resource.labels.stream_id)"
    "database"  = "EXTRACT(jsonPayload.database)"
    "table"     = "EXTRACT(jsonPayload.table)"
  }
}

# Alert policy for schema changes
resource "google_monitoring_alert_policy" "schema_change_alert" {
  display_name = "MATCHING: (${local.environment_name}) okuna db schema change alert"
  combiner     = "OR"

  conditions {
    display_name = "Schema change detected in Okuna DB tables"

    condition_threshold {
      filter     = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.datastream_schema_change.name}\" AND resource.type=\"datastream.googleapis.com/Stream\" AND resource.labels.stream_id=\"${local.environment_name}-okuna-to-bigquery\""
      duration   = "0s"
      comparison = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }

      trigger {
        count = 1
      }
    }
  }

  documentation {
    content = <<-EOT
      ## Schema Change Alert for Okuna Database

      A schema change has been detected in the PostgreSQL database:
      - Environment: ${local.environment_name}
      - Database: ${data.terraform_remote_state.okuna_state.outputs.database_name}
      - Datastream Stream: ${local.environment_name}-okuna-to-bigquery

      **Possible Actions Required:**
      1. Review the schema changes in the source table
      2. Verify BigQuery target table compatibility
      3. Ensure the replication continues to function correctly
      4. Ensure that data pipelines continue to work correctly or if they have to be adopted accordingly

      **Note:** If you recently added tables to the `datastream_okuna_tables` list in Terraform,
      this alert may have been triggered by the configuration change rather than an actual schema change.
    EOT
    mime_type = "text/markdown"
  }

  notification_channels = [local.environment_name == "production" ? data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_production_id : data.terraform_remote_state.holi_infra_monitoring_state.outputs.monitoring_notification_channel_rocket_chat_matching_data_staging_id]

  alert_strategy {
    auto_close = "1800s"  # Auto-close after 30 minutes
  }
}