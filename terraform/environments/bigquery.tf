locals {
  dataset_id_datalake = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_development
  dataset_id_datapool = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_development

  datalake = "${local.default_project}.${local.dataset_id_datalake}"
  datapool = "${local.default_project}.${local.dataset_id_datapool}"

  okuna_dataset = "${local.default_project}.okuna_${local.environment_name}_public"
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
    ignore_changes = [encryption_configuration]
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
    { "name": "uses", "type": "STRING","mode": "REPEATED"},
    { "name": "additionalData", "type": "JSON","mode": "NULLABLE"}
]
EOF
}

resource "google_bigquery_table" "classifications" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "classifications"

  schema = <<EOF
  [
    { "name": "type", "type": "STRING", "mode": "REQUIRED"},
    { "name": "id", "type": "STRING", "mode": "REQUIRED"},
    { "name": "hash", "type": "STRING", "mode": "REQUIRED"},
    { "name": "topics_skills_embedding_v2", "type": "FLOAT", "mode": "REPEATED"},
    { "name": "topics_v2", "type": "STRING", "mode": "REPEATED"},
    { "name": "skills_v2", "type": "STRING", "mode": "REPEATED"},
    { "name": "similarity_embedding", "type": "FLOAT", "mode": "REPEATED"}
  ]
  EOF

  lifecycle {
    ignore_changes = [encryption_configuration]
  }
}

output "bigquery_table_name_classifications" {
  value = "${local.datalake}.${google_bigquery_table.classifications.table_id}"
}

resource "google_bigquery_table" "tasks_without_classifications" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "tasks_without_classifications"

  view {
    use_legacy_sql = false
    # ATTENTION: if you change the hash calculation function, make sure to also change it in
    #  * the JOIN clause AND
    #  * the "tasks_with_classifications" view
    query          = <<SQL
      SELECT
        task.* EXCEPT(skills_v2),
        JSON_VALUE_ARRAY(task.skills_v2) AS skills_v2,
        JSON_VALUE_ARRAY(community.topics_v2) AS topics_v2,
        community.name AS community_slug,
        CAST(FARM_FINGERPRINT(CONCAT(IFNULL(task.name, ''), '|', IFNULL(task.description, ''), '|', IFNULL(task.location, ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), ' '), ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), ' '), ''))) AS STRING) AS `hash`
      FROM `${local.okuna_dataset}.openbook_communities_task` AS task
      JOIN `${local.okuna_dataset}.openbook_communities_community` AS community
        ON task.community_id = community.id
      LEFT JOIN `${local.datalake}.classifications` AS classification
        ON task.id = classification.id
        AND classification.type = 'task'
        AND classification.hash = CAST(FARM_FINGERPRINT(CONCAT(IFNULL(task.name, ''), '|', IFNULL(task.description, ''), '|', IFNULL(task.location, ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), ' '), ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), ' '), ''))) AS STRING)
      WHERE community.is_deleted IS FALSE
        AND visibility="PUBLIC"
        AND deleted_at IS NULL
        AND (
          classification.topics_skills_embedding_v2 IS NULL
          OR classification.similarity_embedding IS NULL
        )
    SQL
  }
}

resource "google_bigquery_table" "tasks_with_classifications" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "tasks_with_classifications"

  view {
    use_legacy_sql = false
    # ATTENTION: if you change the hash calculation function, make sure to also change it in
    #  * the JOIN clause AND
    #  * the "tasks_without_classifications" view
    query          = <<SQL
      SELECT
        task.* EXCEPT(skills_v2),
        classification.* EXCEPT(id, type, `hash`),
        community.name AS community_slug,
        CAST(FARM_FINGERPRINT(CONCAT(IFNULL(task.name, ''), '|', IFNULL(task.description, ''), '|', IFNULL(task.location, ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), ' '), ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), ' '), ''))) AS STRING) AS `hash`
      FROM `${local.okuna_dataset}.openbook_communities_task` AS task
      JOIN `${local.okuna_dataset}.openbook_communities_community` AS community
        ON task.community_id = community.id
      LEFT JOIN `${local.datalake}.classifications` AS classification
        ON task.id = classification.id
        AND classification.type = 'task'
        AND classification.hash = CAST(FARM_FINGERPRINT(CONCAT(IFNULL(task.name, ''), '|', IFNULL(task.description, ''), '|', IFNULL(task.location, ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), ' '), ''), '|', IFNULL(ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), ' '), ''))) AS STRING)
      WHERE community.is_deleted IS FALSE
        AND visibility="PUBLIC"
        AND deleted_at IS NULL
        AND classification.topics_skills_embedding_v2 IS NOT NULL
        AND classification.similarity_embedding IS NOT NULL
    SQL
  }
}

resource "google_bigquery_table" "volunteering_voltastics_without_classifications" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "volunteering_voltastics_without_classifications"

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        raw_deduped.*,
        classifications.topics_skills_embedding_v2,
        classifications.similarity_embedding
      FROM `${local.datalake}.volunteering_voltastics_raw_deduped` raw_deduped
      LEFT OUTER JOIN `${google_bigquery_table.classifications.dataset_id}.${google_bigquery_table.classifications.table_id}` classifications
      ON
        CAST(raw_deduped.id AS STRING) = classifications.id AND
        raw_deduped.hash = classifications.hash AND
        classifications.type = 'volunteering'
      WHERE DATE(raw_deduped.partitiontime) = (
          SELECT MAX(DATE(partitiontime))
          FROM `${local.datalake}.volunteering_voltastics_raw_deduped`
          WHERE partitiontime IS NOT NULL
      )
      AND (
          classifications.hash IS NULL OR
          classifications.topics_skills_embedding_v2 IS NULL OR
          classifications.similarity_embedding IS NULL
      )
    SQL
  }
}

# TODO migrate table / view definitions from holi-data-classification once it is "shut down":
# TODO - volunteering_voltastics_raw_deduped_aggregated_images
# TODO - volunteering_voltastics_raw_deduped
# TODO - location_cache

resource "google_bigquery_table" "volunteering_voltastics_classified_enriched_partitioned" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_classified_enriched_partitioned"
  description = "View holding all (partitioned) volunteering data, together with topics&skill classification, similarity classification and location enrichment. Used by the volunteering-to-typesense pipeline."

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        raw_deduped.* EXCEPT(latitude, longitude),
        COALESCE(raw_deduped.latitude, location_cache.latitude) AS latitude,
        COALESCE(raw_deduped.longitude, location_cache.longitude) AS longitude,
        classifications.topics_skills_embedding_v2 AS topics_skills_embedding_v2,
        classifications.topics_v2 AS topics_v2,
        classifications.skills_v2 AS skills_v2,
        classifications.similarity_embedding AS similarity_embedding
      FROM
        `${local.datalake}.volunteering_voltastics_raw_deduped` AS raw_deduped
      LEFT OUTER JOIN
        `${local.datalake}.location_cache` AS location_cache
        ON raw_deduped.location = location_cache.location
      LEFT OUTER JOIN `${local.datalake}.classifications` AS classifications
        ON CAST(raw_deduped.id AS STRING) = classifications.id
        AND raw_deduped.hash = classifications.hash
    SQL
  }
}


output "volunteering_voltastics_raw_table" {
  value = google_bigquery_table.volunteering_voltastics_raw.table_id
}

output "volunteering_voltastics_raw_table_full" {
  value = "${google_bigquery_table.volunteering_voltastics_raw.project}.${google_bigquery_table.volunteering_voltastics_raw.dataset_id}.${google_bigquery_table.volunteering_voltastics_raw.table_id}"
}

resource "google_bigquery_table" "donations_betterplace_raw" {
  project    = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "donations_betterplace_raw"


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

output "donations_betterplace_raw_table" {
  value = google_bigquery_table.donations_betterplace_raw.table_id
}

output "donations_betterplace_raw_table_full" {
  value = "${google_bigquery_table.donations_betterplace_raw.project}.${google_bigquery_table.donations_betterplace_raw.dataset_id}.${google_bigquery_table.donations_betterplace_raw.table_id}"
}