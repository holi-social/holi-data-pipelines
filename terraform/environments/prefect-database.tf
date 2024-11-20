resource "google_sql_database" "database" {
  name     = "prefect-${local.environment_name}"
  project  = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  instance = local.environment_name == "production" ? data.terraform_remote_state.holi_infra_state.outputs.database_name_production : data.terraform_remote_state.holi_infra_state.outputs.database_name_development
}

output "database_name" {
  value = google_sql_database.database.name
}
