locals {
  datastream_subnet = "192.168.100.0/24"
}

data "google_project" "shared" {
  project_id = local.default_project
}

resource "google_kms_crypto_key_iam_member" "datastream_serviceaccount" {
  crypto_key_id = google_kms_crypto_key.data_pipeline.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.shared.number}@gcp-sa-datastream.iam.gserviceaccount.com"
}

resource "google_datastream_private_connection" "datastream_peering" {
  display_name          = "VPC peering with network default"
  location              = local.default_region
  private_connection_id = "peering-with-network-default"

  vpc_peering_config {
    vpc    = data.terraform_remote_state.holi_infra_state.outputs.google_compute_network_default_id
    subnet = local.datastream_subnet
  }
}

resource "google_compute_firewall" "datastream_nat_vm" {
  name          = "datastream-nat-vm"
  description   = "Allow datastream to access the NAT VM for staging and production ports"
  network       = data.terraform_remote_state.holi_infra_state.outputs.google_compute_network_default_name
  source_ranges = [local.datastream_subnet]
  priority      = 65534
  target_tags   = ["datastream-nat"]

  allow {
    protocol = "tcp"
    ports    = [5432]
  }
}

output "datastream_peering_id" {
  value = google_datastream_private_connection.datastream_peering.id
}
