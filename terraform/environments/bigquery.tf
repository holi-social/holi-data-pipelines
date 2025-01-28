locals {
  dataset_id_datalake = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_development
  dataset_id_datapool = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_development

  datalake = "${local.default_project}.${local.dataset_id_datalake}"
  datapool = "${local.default_project}.${local.dataset_id_datapool}"
}

resource "google_bigquery_table" "volunteering_voltastics_raw" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw"


  time_partitioning {
    type          = "DAY"
    expiration_ms = 2592000000 # 30 days
  }

  deletion_protection = true

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

resource "google_bigquery_table" "donations_betterplace_raw" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "donations_betterplace_raw_playground"


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
   {"name": "links","type": "RECORD", "mode": "REPEATED", "fields": [
         {"name": "templated","type": "BOOLEAN","mode": "NULLABLE" },
         { "name": "href","type": "STRING", "mode": "NULLABLE" },
         { "name": "rel", "type": "STRING", "mode": "NULLABLE" } ] },
   { "name": "profile_picture", "type": "RECORD", "mode": "NULLABLE", "fields": [
         {"name": "fallback","type": "BOOLEAN","mode": "NULLABLE"},
         { "name": "links","type": "RECORD","mode": "REPEATED", "fields": [
               {  "name": "href", "type": "STRING", "mode": "NULLABLE" },
               {  "name": "rel","type": "STRING", "mode": "NULLABLE" } ]}]},
   { "name": "contact", "type": "RECORD", "mode": "NULLABLE", "fields": [
         { "name": "links", "type": "RECORD", "mode": "REPEATED", "fields": [
               { "name": "href", "type": "STRING", "mode": "NULLABLE"},
               { "name": "rel", "type": "STRING", "mode": "NULLABLE"}]},
         { "name": "picture", "type": "RECORD", "mode": "NULLABLE", "fields": [
               { "name": "links", "type": "RECORD", "mode": "REPEATED", "fields": [
                     {"name": "href", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "rel", "type": "STRING", "mode": "NULLABLE"}]},
               { "name": "fallback", "type": "BOOLEAN", "mode": "NULLABLE"}]},
         { "name": "name", "type": "STRING", "mode": "NULLABLE"},
         { "name": "id", "type": "INTEGER", "mode": "NULLABLE"}]},
   { "name": "completed_need_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "comments_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "donor_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "donations_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "carrier", "type": "RECORD", "mode": "NULLABLE", "fields": [
         { "name": "links", "type": "RECORD", "mode": "REPEATED", "fields": [
               { "name": "href", "type": "STRING", "mode": "NULLABLE"},
               { "name": "rel", "type": "STRING", "mode": "NULLABLE"}]},
         { "name": "country", "type": "STRING", "mode": "NULLABLE"},
         { "name": "picture", "type": "RECORD", "mode": "NULLABLE", "fields": [
               { "name": "links", "type": "RECORD", "mode": "REPEATED", "fields": [
                     {"name": "href", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "rel", "type": "STRING", "mode": "NULLABLE"}]},
               { "name": "fallback", "type": "BOOLEAN", "mode": "NULLABLE"}]},
         { "name": "id", "type": "INTEGER", "mode": "NULLABLE"},
         { "name": "city", "type": "STRING", "mode": "NULLABLE"},
         { "name": "name", "type": "STRING", "mode": "NULLABLE"}]},
   { "name": "id", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "completed_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
   { "name": "description", "type": "STRING", "mode": "NULLABLE" },
   { "name": "open_amount_in_cents", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "tax_deductible", "type": "BOOLEAN", "mode": "NULLABLE" },
   { "name": "hash", "type": "STRING", "mode": "NULLABLE" },
   { "name": "title", "type": "STRING", "mode": "NULLABLE" },
   { "name": "negative_opinions_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "city", "type": "STRING", "mode": "NULLABLE" },
   { "name": "activated_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
   { "name": "active_matching_fund", "type": "RECORD", "mode": "NULLABLE", "fields": [
               { "name": "links", "type": "RECORD", "mode": "REPEATED", "fields": [
                     {"name": "href", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "rel", "type": "STRING", "mode": "NULLABLE"}]},
         { "name": "id", "type": "INTEGER", "mode": "NULLABLE"},
         { "name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
         { "name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
         { "name": "activated_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
         { "name": "title", "type": "STRING", "mode": "NULLABLE"},
         { "name": "description", "type": "STRING", "mode": "NULLABLE"},
         { "name": "company_name", "type": "STRING", "mode": "NULLABLE"},
         { "name": "client_id", "type": "STRING", "mode": "NULLABLE"},
         { "name": "provided_amount_in_cents", "type": "INTEGER", "mode": "NULLABLE"},
         { "name": "donated_amount_in_cents", "type": "INTEGER", "mode": "NULLABLE"},
         { "name": "state", "type": "STRING", "mode": "NULLABLE"},
         { "name": "logo_url", "type": "STRING", "mode": "NULLABLE"},
         { "name": "maximum_matching_amount_in_cents", "type": "INTEGER", "mode": "NULLABLE"}]},
   { "name": "zip", "type": "STRING", "mode": "NULLABLE" },
   { "name": "incomplete_need_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "newsletter_subscriptions_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
   { "name": "content_updated_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
   { "name": "positive_opinions_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "blog_post_count", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "progress_percentage", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "closed_at", "type": "TIMESTAMP", "mode": "NULLABLE" },
   { "name": "latitude", "type": "FLOAT", "mode": "NULLABLE" },
   { "name": "country", "type": "STRING", "mode": "NULLABLE" },
   { "name": "summary", "type": "STRING", "mode": "NULLABLE" },
   { "name": "matching_events", "type": "RECORD", "mode": "REPEATED", "fields": [
         { "name": "banner_text_color", "type": "STRING", "mode": "NULLABLE"},
         { "name": "banner_background_color", "type": "STRING", "mode": "NULLABLE"},
         { "name": "links", "type": "STRING", "mode": "REPEATED"},
         { "name": "matching_percentage", "type": "INTEGER", "mode": "NULLABLE"},
         { "name": "id", "type": "INTEGER", "mode": "NULLABLE"}]},
   { "name": "street", "type": "STRING", "mode": "NULLABLE" },
   { "name": "categories", "type": "RECORD", "mode": "REPEATED", "fields": [
         { "name": "slug", "type": "STRING", "mode": "NULLABLE"},
         { "name": "name", "type": "STRING", "mode": "NULLABLE"},
         { "name": "id", "type": "INTEGER", "mode": "NULLABLE"}]},
   { "name": "donations_prohibited", "type": "BOOLEAN", "mode": "NULLABLE" },
   { "name": "longitude", "type": "FLOAT", "mode": "NULLABLE" },
   { "name": "donated_amount_in_cents", "type": "INTEGER", "mode": "NULLABLE" },
   { "name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF
}
