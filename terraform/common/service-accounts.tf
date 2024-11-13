resource "google_service_account" "data_pipeline" {
  account_id   = "data-pipeline"
  display_name = "Service Account for the data pipeline"
}

resource "google_project_iam_member" "data_pipeline_sa_user" {
  project = local.default_project
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.data_pipeline.email}"
}

resource "google_project_iam_member" "data_pipeline_cloud_run" {
  project = local.default_project
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.data_pipeline.email}"
}

output "data_pipeline_service_account_email" {
  value = google_service_account.data_pipeline.email
}

output "data_pipeline_service_account_name" {
  value = google_service_account.data_pipeline.name
}
