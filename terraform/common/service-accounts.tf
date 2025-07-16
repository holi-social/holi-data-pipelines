resource "google_service_account" "data_pipeline" {
  account_id   = "data-pipeline"
  display_name = "Service Account for the data pipeline"
}

resource "google_project_iam_member" "data_pipeline" {
  project = local.default_project
  for_each = toset([
    "roles/iam.serviceAccountUser",
    "roles/run.developer",
    "roles/bigquery.user",
    "roles/bigquery.dataEditor", # TODO try to replace with a more specific role
    "roles/secretmanager.secretAccessor",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/aiplatform.user",
    "roles/artifactregistry.reader",
  ])
  role   = each.key
  member = "serviceAccount:${google_service_account.data_pipeline.email}"

}

# this account was imported after default creation by google
data "google_compute_default_service_account" "compute_default" {
}

# This exclusively binds the role to the specific service account. If you ever need to bind this role to other 
# members, either do it here or convert both occurrences to member resources.
resource "google_service_account_iam_member" "compute_default" {
  service_account_id = data.google_compute_default_service_account.compute_default.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.data_pipeline.email}"
}

# This exclusively binds the role to the specific service account. If you ever need to bind this role to other 
# members, either do it here or convert both occurrences to member resources.
resource "google_service_account_iam_binding" "data_pipeline" {
  service_account_id = google_service_account.data_pipeline.name
  role               = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${data.terraform_remote_state.holi_infra_state.outputs.terraform_service_account_email}",
  ]
}

output "data_pipeline_service_account_email" {
  value = google_service_account.data_pipeline.email
}

output "data_pipeline_service_account_name" {
  value = google_service_account.data_pipeline.name
}
