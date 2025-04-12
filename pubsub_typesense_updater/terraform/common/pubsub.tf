resource "google_project_iam_member" "pubsub_subscriber" {
  for_each = local.service_account_ids
  project  = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  member   = "serviceAccount:${each.value}"
  role     = "roles/pubsub.subscriber"
}
