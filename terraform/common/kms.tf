resource "google_kms_crypto_key" "data_pipeline" {
  name     = "data-pipeline-key"
  key_ring = data.terraform_remote_state.holi_infra_state.outputs.kms_shared_key_ring_id

  lifecycle {
    prevent_destroy = true # if destroyed, data is inaccessible forever
  }
}

resource "google_kms_crypto_key_iam_member" "data_pipeline" {
  crypto_key_id = google_kms_crypto_key.data_pipeline.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.terraform_remote_state.holi_infra_state.outputs.shared_project_number}@compute-system.iam.gserviceaccount.com"
}

output "data_pipeline_key" {
  value = google_kms_crypto_key.data_pipeline.id
}

