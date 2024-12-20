locals {
  dataset_id_datalake = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_development
  dataset_id_datapool = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_development

  datalake = "${local.default_project}.${local.dataset_id_datalake}"
  datapool = "${local.default_project}.${local.dataset_id_datapool}"




}

resource "google_bigquery_table" "volunteering_voltastics_raw" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw_playground"


  time_partitioning {
    type          = "DAY"
    expiration_ms = 2592000000 # 30 days
  }

  deletion_protection = false

  lifecycle {
    ignore_changes = [encryption_configuration, schema]
  }

  schema = <<EOF
[
    { "name": "longitude", "type": "FLOAT", "mode": "NULLABLE" },
    { "name": "imageOrga", "type": "STRING", "mode": "NULLABLE"},
    { "name": "location",  "type": "STRING", "mode": "NULLABLE"},
    { "name": "title","type": "STRING","mode": "NULLABLE"},
    { "name": "source", "type": "STRING", "mode": "NULLABLE"},
    { "name": "hashTags", "type": "STRING", "mode": "REPEATED"},
    { "name": "description", "type": "STRING","mode": "NULLABLE"},
    { "name": "image", "type": "STRING","mode": "NULLABLE"},
    { "name": "link", "type": "STRING", "mode": "NULLABLE"},
    { "name": "orga", "type": "STRING", "mode": "NULLABLE"},
    { "name": "id", "type": "INTEGER","mode": "NULLABLE"},
    { "name": "latitude","type": "FLOAT", "mode": "NULLABLE"},
    { "name": "uses", "type": "STRING","mode": "REPEATED"}
]
EOF
}
