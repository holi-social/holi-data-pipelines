# this account was imported after default creation by google
data "google_compute_default_service_account" "compute_default" {
}

# This exclusively binds the role to the specific service account. If you ever need to bind this role to other 
# members, either do it here or convert both occurrences to member resources.
resource "google_service_account_iam_member" "compute_default" {
  service_account_id = data.google_compute_default_service_account.compute_default.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email}"
}

# This exclusively binds the role to the specific service account. If you ever need to bind this role to other 
# members, either do it here or convert both occurrences to member resources.
resource "google_service_account_iam_binding" "data_pipeline" {
  service_account_id = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_name
  role               = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${data.terraform_remote_state.holi_infra_state.outputs.terraform_service_account_email}",
  ]
}

resource "google_service_account_key" "data_pipeline" {
  service_account_id = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# TODO green/blue
# TODO green/blue certificate replication
resource "google_compute_instance" "prefect_server_green" {
  name         = "prefect-server-${local.environment}-green"
  machine_type = "t2d-standard-2"
  zone         = "${local.default_region}-a"
  tags         = ["http-server", "https-server", "team-matching", "data-pipeline", local.environment_type]

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

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    startup-script = templatefile("prefect-server-setup.tftpl", {
      json_key = base64decode(google_service_account_key.data_pipeline.private_key),
      domain   = local.prefect_dns_domain
    })
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.data_pipeline_service_account_email
    scopes = ["compute-rw"]
  }
}

resource "google_dns_record_set" "prefect_data_holi_social" {
  name         = "${local.prefect_dns_domain}."
  type         = "A"
  ttl          = 300
  managed_zone = data.terraform_remote_state.holi_data_pipeline_common_state.outputs.dns_prefect_domain_name
  rrdatas      = [google_compute_instance.prefect_server_green.network_interface[0].access_config[0].nat_ip]
}
