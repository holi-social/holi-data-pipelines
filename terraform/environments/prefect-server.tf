data "google_compute_subnetwork" "default-region" {
  name   = data.terraform_remote_state.holi_infra_state.outputs.google_compute_network_default_name
  region = local.default_region
}

data "google_secret_manager_secret_version_access" "prefect_database_username" {
  secret = local.environment == "production" ? "PREFECT_DB_USERNAME_PRODUCTION" : "PREFECT_DB_USERNAME_DEVELOPMENT"
}

data "google_secret_manager_secret_version_access" "prefect_database_password" {
  secret = local.environment == "production" ? "PREFECT_DB_PASSWORD_PRODUCTION" : "PREFECT_DB_PASSWORD_DEVELOPMENT"
}

data "local_file" "prefect_server_requirements_file" {
  filename = "prefect-server-requirements.txt"
}

# TODO green/blue
# TODO green/blue certificate replication
resource "google_compute_instance" "prefect_server_green" {
  name         = "prefect-server-${local.environment}-green"
  machine_type = "e2-small" # TODO re-evaluate (e.g. from observability data) whether this machine type is enough...
  zone         = "${local.default_region}-a"

  boot_disk {
    kms_key_self_link = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_key
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
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
    startup-script = templatefile("prefect-server-setup.tftpl", {
      internal_domain    = local.prefect_internal_domain
      internal_port      = local.prefect_internal_port
      allowed_cidr_range = data.google_compute_subnetwork.default-region.ip_cidr_range
      database_username  = data.google_secret_manager_secret_version_access.prefect_database_username.secret_data
      database_password  = data.google_secret_manager_secret_version_access.prefect_database_password.secret_data
      database_name      = google_sql_database.database.name
      database_host      = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.database_ip_address_production : data.terraform_remote_state.holi_infra_state.outputs.database_ip_address_development
      requirements       = data.local_file.prefect_server_requirements_file.content
    })
  }

  labels = {
    purpose : "data-pipelines"
    environment : local.environment_name
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email
    scopes = [
      "compute-rw",
      "logging-write",
      "monitoring-write",
      "pubsub",
    ]
  }
}


resource "google_dns_record_set" "prefect_server_holi_internal" {
  name         = "${local.prefect_internal_domain}."
  type         = "A"
  ttl          = 300
  managed_zone = data.terraform_remote_state.holi_infra_state.outputs.dns_internal_zone_name
  rrdatas      = [google_compute_instance.prefect_server_green.network_interface[0].network_ip]
}

output "prefect_server_url" {
  value = local.prefect_server_url
}
