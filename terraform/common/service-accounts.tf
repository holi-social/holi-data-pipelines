resource "google_service_account" "data_pipeline" {
  account_id   = "data-pipeline"
  display_name = "Service Account for the data pipeline"
}

resource "google_project_iam_member" "data_pipeline" {
  project = local.default_project
  for_each = toset([
    "roles/iam.serviceAccountUser",
    "roles/run.developer",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ])
  role   = each.key
  member = "serviceAccount:${google_service_account.data_pipeline.email}"

}

output "data_pipeline_service_account_email" {
  value = google_service_account.data_pipeline.email
}

output "data_pipeline_service_account_name" {
  value = google_service_account.data_pipeline.name
}
