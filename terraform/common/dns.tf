locals {
  # effectively, this is data.holi.social
  data_zone_dns_name    = "data.${data.terraform_remote_state.holi_infra_state.outputs.dns_sld_zone_domain}."
  prefect_zone_dns_name = "prefect.${google_dns_managed_zone.data_environments_zone.dns_name}"
}

# data environments zone
resource "google_dns_managed_zone" "data_environments_zone" {
  project  = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name     = "data-holi-social-environments-zone"
  dns_name = local.data_zone_dns_name
}

resource "google_dns_record_set" "data_environments_zone_entry" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name    = local.data_zone_dns_name
  type    = "NS"
  ttl     = 300

  managed_zone = data.terraform_remote_state.holi_infra_state.outputs.dns_sld_zone_name

  rrdatas = google_dns_managed_zone.data_environments_zone.name_servers
}

resource "google_dns_managed_zone" "prefect_environments_zone" {
  project  = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name     = "prefect-data-holi-social-environments-zone"
  dns_name = local.prefect_zone_dns_name
}

resource "google_dns_record_set" "prefect_environments_zone_entry" {
  project = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  name    = local.prefect_zone_dns_name
  type    = "NS"
  ttl     = 300

  managed_zone = google_dns_managed_zone.data_environments_zone.name

  rrdatas = google_dns_managed_zone.prefect_environments_zone.name_servers
}

output "dns_data_domain" {
  value = trim(google_dns_managed_zone.data_environments_zone.dns_name, ".")
}

output "dns_data_domain_name" {
  value = google_dns_managed_zone.data_environments_zone.name
}

output "dns_prefect_domain" {
  value = trim(google_dns_managed_zone.prefect_environments_zone.dns_name, ".")
}

output "dns_prefect_domain_name" {
  value = google_dns_managed_zone.prefect_environments_zone.name
}
