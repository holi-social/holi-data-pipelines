# see https://cloud.google.com/functions/docs/tutorials/terraform-pubsub

resource "google_service_account" "search_integration" {
  project      = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  account_id   = "holi-search-integration-sa"
  display_name = "holi-search-integration Service Account"
}

locals {
  service_account_ids = {
    cloudfunctions_admin_robot = "service-984061681268@gcf-admin-robot.iam.gserviceaccount.com"
    cloudbuild                 = "984061681268@cloudbuild.gserviceaccount.com"
    search_integration         = google_service_account.search_integration.email
  }
}

resource "google_project_iam_member" "secretmanager_accessor" {
  for_each = local.service_account_ids
  project  = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  member   = "serviceAccount:${each.value}"
  role     = "roles/secretmanager.secretAccessor"
}

resource "google_project_iam_member" "function_invoker" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  member  = "serviceAccount:${local.service_account_ids.search_integration}"
  role    = "roles/run.invoker"
}

output "function_service_account" {
  value = local.service_account_ids.search_integration
}
