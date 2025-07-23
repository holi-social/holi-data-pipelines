locals {
  project_id = data.terraform_remote_state.holi_infra_state.outputs.shared_project_id

  dataset_id_datalake = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_development
  dataset_id_datapool = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_development

  datalake = "${local.default_project}.${local.dataset_id_datalake}"
  datapool = "${local.default_project}.${local.dataset_id_datapool}"

  okuna_dataset = "${local.default_project}.okuna_${local.environment_name}_public"

  aktion_mensch_base_url = "https://www.aktion-mensch.de/was-du-tun-kannst/ehrenamt/engagement-plattform#/lokale-angebote/"
}

resource "google_bigquery_table" "location_cache" {
  project    = local.project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "location_cache"

  schema = <<EOF
  [
    { "name": "location", "type": "STRING", "mode": "REQUIRED"},
    { "name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    { "name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    { "name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE", "defaultExpression": "CURRENT_TIMESTAMP()" }
  ]
  EOF

  lifecycle {
    ignore_changes = [encryption_configuration]
  }
}

// UDF for Aktion Mensch hash calculation
resource "google_bigquery_routine" "calculate_aktion_mensch_hash" {
  project         = local.project_id
  dataset_id      = local.dataset_id_datalake
  routine_id      = "calculate_aktion_mensch_hash"
  routine_type    = "SCALAR_FUNCTION"
  language        = "SQL"
  definition_body = <<-SQL
    CAST(FARM_FINGERPRINT(CONCAT(
      IFNULL(titel, ''), '|', 
      IFNULL(beschreibung, ''), '|', 
      IFNULL(kurzbeschreibung, ''), '|',
      IFNULL(JSON_EXTRACT_SCALAR(durchfuehrendeOrganisation, "$.name"), ''), '|',
      IFNULL(TO_JSON_STRING(verortung), ''), '|',
      IFNULL(JSON_EXTRACT_SCALAR(einsatzort, "$.plz"), ''), '|',
      IFNULL(JSON_EXTRACT_SCALAR(einsatzort, "$.ort"), ''), '|',
      IFNULL(TO_JSON_STRING(attribute), ''), '|',
      IFNULL(JSON_EXTRACT_SCALAR(kontaktDurchfuehrendeOrganisation, "$.email"), ''), '|',
      IFNULL(JSON_EXTRACT_SCALAR(kontaktDurchfuehrendeOrganisation, "$.telefon"), ''), '|',
      IFNULL(TO_JSON_STRING(kontaktVermittelndeStelle), '')
    )) AS STRING)
  SQL

  arguments {
    name      = "titel"
    data_type = "{\"typeKind\": \"STRING\"}"
  }
  arguments {
    name      = "beschreibung"
    data_type = "{\"typeKind\": \"STRING\"}"
  }
  arguments {
    name      = "kurzbeschreibung"
    data_type = "{\"typeKind\": \"STRING\"}"
  }
  arguments {
    name      = "durchfuehrendeOrganisation"
    data_type = "{\"typeKind\": \"JSON\"}"
  }
  arguments {
    name      = "verortung"
    data_type = "{\"typeKind\": \"JSON\"}"
  }
  arguments {
    name      = "einsatzort"
    data_type = "{\"typeKind\": \"JSON\"}"
  }
  arguments {
    name      = "attribute"
    data_type = "{\"typeKind\": \"JSON\"}"
  }
  arguments {
    name      = "kontaktDurchfuehrendeOrganisation"
    data_type = "{\"typeKind\": \"JSON\"}"
  }
  arguments {
    name      = "kontaktVermittelndeStelle"
    data_type = "{\"typeKind\": \"JSON\"}"
  }

  return_type = "{\"typeKind\": \"STRING\"}"
}

resource "google_bigquery_table" "volunteering_voltastics_raw" {
  project    = local.project_id
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
  project    = local.project_id
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
    { "name": "similarity_embedding", "type": "FLOAT", "mode": "REPEATED"},
    { "name": "commitment_label", "type": "STRING", "mode": "NULLABLE" },
    { "name": "location_type", "type": "STRING", "mode": "NULLABLE" },
    { "name": "commitment_duration", "type": "STRING", "mode": "NULLABLE" },
    { "name": "commitment_duration_ai_reasoning", "type": "STRING", "mode": "NULLABLE" },
    { "name": "classification_failed", "type": "BOOLEAN", "mode": "NULLABLE" }
  ]
  EOF

  lifecycle {
    ignore_changes = [encryption_configuration]
  }
}

resource "google_bigquery_table" "tasks_without_classifications" {
  project    = local.project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "tasks_without_classifications"

  view {
    use_legacy_sql = false
    # ATTENTION: if you change the hash calculation function, make sure to also change it in
    #  * the JOIN clause AND
    #  * the "tasks_with_classifications" view AND
    #  * the "farm_fingerprint" function in pubsub_typesense_updater/events.py
    query = <<SQL
      SELECT
        task.* EXCEPT(skills_v2),
        classification.topics_skills_embedding_v2 as topics_skills_embedding_v2,
        classification.similarity_embedding as similarity_embedding,
        JSON_VALUE_ARRAY(task.skills_v2) AS skills_v2,
        JSON_VALUE_ARRAY(community.topics_v2) AS topics_v2,
        community.name AS community_slug,
        community.title AS community_name,
        community.avatar AS community_avatar,
        CAST(FARM_FINGERPRINT(CONCAT(task.name, '|', task.description, '|', IFNULL(task.location, ''), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), '\', \'', ''), '\']'), '[]'), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), '\', \'', ''), '\']'), '[]'))) AS STRING) AS `hash`,
      FROM `${local.okuna_dataset}.openbook_communities_task` AS task
      JOIN `${local.okuna_dataset}.openbook_communities_community` AS community
        ON task.community_id = community.id
      LEFT JOIN `${local.datalake}.classifications` AS classification
        ON task.id = classification.id
        AND classification.type = 'task'
        AND classification.hash = CAST(FARM_FINGERPRINT(CONCAT(task.name, '|', task.description, '|', IFNULL(task.location, ''), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), '\', \'', ''), '\']'), '[]'), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), '\', \'', ''), '\']'), '[]'))) AS STRING)
      WHERE community.is_deleted IS FALSE
        AND visibility="PUBLIC"
        AND deleted_at IS NULL
        AND (
          classification.topics_skills_embedding_v2 IS NULL
          OR classification.similarity_embedding IS NULL
        )
        AND (
          classification.classification_failed IS NULL
          OR classification.classification_failed IS FALSE
        )
    SQL
  }
}

resource "google_bigquery_table" "tasks_with_classifications" {
  project    = local.project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "tasks_with_classifications"

  view {
    use_legacy_sql = false
    # ATTENTION: if you change the hash calculation function, make sure to also change it in
    #  * the JOIN clause AND
    #  * the "tasks_without_classifications" view AND
    #  * the "farm_fingerprint" function in pubsub_typesense_updater/events.py
    query = <<SQL
      SELECT
        task.* EXCEPT(skills_v2),
        classification.* EXCEPT(id, type, `hash`, location_type),
        community.name AS community_slug,
        community.title AS community_name,
        community.avatar AS community_avatar,
        CAST(FARM_FINGERPRINT(CONCAT(task.name, '|', task.description, '|', IFNULL(task.location, ''), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), '\', \'', ''), '\']'), '[]'), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), '\', \'', ''), '\']'), '[]'))) AS STRING) AS `hash`
      FROM `${local.okuna_dataset}.openbook_communities_task` AS task
      JOIN `${local.okuna_dataset}.openbook_communities_community` AS community
        ON task.community_id = community.id
      LEFT JOIN `${local.datalake}.classifications` AS classification
        ON task.id = classification.id
        AND classification.type = 'task'
        AND classification.hash = CAST(FARM_FINGERPRINT(CONCAT(task.name, '|', task.description, '|', IFNULL(task.location, ''), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(community.topics_v2), '\', \'', ''), '\']'), '[]'), '|', IFNULL(CONCAT('[\'', ARRAY_TO_STRING(JSON_VALUE_ARRAY(task.skills_v2), '\', \'', ''), '\']'), '[]'))) AS STRING)
      WHERE community.is_deleted IS FALSE
        AND visibility="PUBLIC"
        AND deleted_at IS NULL
        AND classification.topics_skills_embedding_v2 IS NOT NULL
        AND classification.similarity_embedding IS NOT NULL
    SQL
  }

  depends_on = [google_bigquery_table.classifications]

}

resource "google_bigquery_table" "volunteering_voltastics_raw_deduped_aggregated_images" {
  project    = local.project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw_deduped_aggregated_images"

  view {
    use_legacy_sql = false
    query = templatefile(
      "view-templates/volunteering_voltastics_raw_deduped_aggregated_images.tftpl",
      {
        raw_table = "${local.datalake}.${google_bigquery_table.volunteering_voltastics_raw.table_id}"
      }
    )
  }
}

resource "google_bigquery_table" "volunteering_voltastics_raw_deduped" {
  project    = local.project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw_deduped"

  view {
    use_legacy_sql = false
    query = templatefile(
      "view-templates/volunteering_voltastics_raw_deduped.tftpl",
      {
        raw_aggregated_table = "${local.datalake}.${google_bigquery_table.volunteering_voltastics_raw_deduped_aggregated_images.table_id}"
      }
    )
  }
}

resource "google_bigquery_table" "volunteering_voltastics_without_classifications" {
  project    = local.project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "volunteering_voltastics_without_classifications"

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        raw_deduped.*,
        classifications.topics_skills_embedding_v2 as topics_skills_embedding_v2,
        classifications.similarity_embedding as similarity_embedding
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
      AND (
        classifications.classification_failed IS NULL
        OR classifications.classification_failed IS FALSE
      )
      # Aktion Mensch is excluded because it is being sourced directly from Aktion Mensch.
      # Excluding it in this view implicates that rows from Aktion Mensch are not classified,
      # thereby saving money and runtime.
      AND raw_deduped.source != 'Aktion Mensch'
    SQL
  }
}

resource "google_bigquery_table" "volunteering_voltastics_classified_enriched_partitioned" {
  project     = local.project_id
  dataset_id  = local.dataset_id_datalake
  table_id    = "volunteering_voltastics_classified_enriched_partitioned"
  description = "View holding all (partitioned) volunteering data, together with topics&skill classification, similarity classification and location enrichment. Used by the volunteering-to-typesense pipeline."

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        raw_deduped.* EXCEPT(latitude, longitude),
        COALESCE(raw_deduped.latitude, location_cache.latitude) AS latitude,
        COALESCE(raw_deduped.longitude, location_cache.longitude) AS longitude,
        NULL as contact,
        NULL as linkOrga,
        classifications.topics_skills_embedding_v2 AS topics_skills_embedding_v2,
        classifications.topics_v2 AS topics_v2,
        classifications.skills_v2 AS skills_v2,
        classifications.similarity_embedding AS similarity_embedding,
        classifications.commitment_label AS commitment_label,
        classifications.location_type AS location_type,
        classifications.commitment_duration AS commitment_duration,
        classifications.commitment_duration_ai_reasoning AS commitment_duration_ai_reasoning,
        classifications.classification_failed as classification_failed,
      FROM
        `${local.datalake}.volunteering_voltastics_raw_deduped` AS raw_deduped
      LEFT OUTER JOIN
        `${local.datalake}.location_cache` AS location_cache
        ON raw_deduped.location = location_cache.location
      LEFT OUTER JOIN `${local.datalake}.classifications` AS classifications
        ON CAST(raw_deduped.id AS STRING) = classifications.id
        AND raw_deduped.hash = classifications.hash
      # Aktion Mensch is excluded because it is being sourced directly from Aktion Mensch.
      # Excluding it in this view implicates that rows from Aktion Mensch are not classified,
      # thereby saving money and runtime.
      WHERE raw_deduped.source != 'Aktion Mensch'
    SQL
  }
}

resource "google_bigquery_table" "volunteering_aktion_mensch_raw" {
  project    = local.project_id
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_aktion_mensch_raw"

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
    { "name": "id", "type": "STRING", "mode": "NULLABLE" },
    { "name": "titel", "type": "STRING", "mode": "NULLABLE" },
    { "name": "kurzbeschreibung", "type": "STRING", "mode": "NULLABLE" },
    { "name": "beschreibung", "type": "STRING", "mode": "NULLABLE" },
    { "name": "barrierefreiheit", "type": "STRING", "mode": "NULLABLE" },
    { "name": "durchfuehrendeOrganisation", "type": "JSON", "mode": "NULLABLE" },
    { "name": "bild", "type": "JSON", "mode": "NULLABLE" },
    { "name": "attribute", "type": "JSON", "mode": "NULLABLE" },
    { "name": "verortung", "type": "JSON", "mode": "NULLABLE" },
    { "name": "einsatzort", "type": "JSON", "mode": "NULLABLE" },
    { "name": "geoposition", "type": "JSON", "mode": "NULLABLE" },
    { "name": "kontaktDurchfuehrendeOrganisation", "type": "JSON", "mode": "NULLABLE" },
    { "name": "kontaktVermittelndeStelle", "type": "JSON", "mode": "NULLABLE" }
]
EOF
}

resource "google_bigquery_table" "volunteering_aktion_mensch_without_classifications" {
  project    = local.project_id
  dataset_id = local.dataset_id_datapool
  table_id   = "volunteering_aktion_mensch_without_classifications"

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        raw.*,
        classifications.topics_skills_embedding_v2,
        classifications.similarity_embedding,
        `${local.datalake}.calculate_aktion_mensch_hash`(
          raw.titel,
          raw.beschreibung,
          raw.kurzbeschreibung,
          raw.durchfuehrendeOrganisation,
          raw.verortung,
          raw.einsatzort,
          raw.attribute,
          raw.kontaktDurchfuehrendeOrganisation,
          raw.kontaktVermittelndeStelle
        ) AS `hash`
      FROM `${local.datalake}.volunteering_aktion_mensch_raw` raw
      LEFT OUTER JOIN `${google_bigquery_table.classifications.dataset_id}.${google_bigquery_table.classifications.table_id}` classifications
      ON
        raw.id = classifications.id AND
        `${local.datalake}.calculate_aktion_mensch_hash`(
          raw.titel,
          raw.beschreibung,
          raw.kurzbeschreibung,
          raw.durchfuehrendeOrganisation,
          raw.verortung,
          raw.einsatzort,
          raw.attribute,
          raw.kontaktDurchfuehrendeOrganisation,
          raw.kontaktVermittelndeStelle
        ) = classifications.hash AND
        classifications.type = 'aktion_mensch_volunteering'
      WHERE DATE(raw._PARTITIONDATE) = (
        SELECT MAX(DATE(_PARTITIONDATE))
        FROM `${local.datalake}.volunteering_aktion_mensch_raw`
        WHERE _PARTITIONDATE IS NOT NULL
      )
      AND (
        classifications.hash IS NULL
        OR classifications.topics_skills_embedding_v2 IS NULL
        OR classifications.similarity_embedding IS NULL
      )
      AND (
        classifications.classification_failed IS NULL
        OR classifications.classification_failed IS FALSE
      )
    SQL
  }
}

# IMPORTANT: The schema of this view must match the schema of volunteering_voltastics_classified_enriched_partitioned
# so that both can be treated uniformly by downstream components (e.g., Typesense ingestion pipeline).
# Any changes to the schema should be synchronized across both views.
resource "google_bigquery_table" "volunteering_aktion_mensch_classified_enriched_partitioned" {
  project     = local.project_id
  dataset_id  = local.dataset_id_datalake
  table_id    = "volunteering_aktion_mensch_classified_enriched_partitioned"
  description = "View joining Aktion Mensch volunteering data with classifications, for ingestion to Typesense. Used by the aktion-mensch-to-typesense pipeline."

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        am.id,
        `${local.datalake}.calculate_aktion_mensch_hash`(
          am.titel,
          am.beschreibung,
          am.kurzbeschreibung,
          am.durchfuehrendeOrganisation,
          am.verortung,
          am.einsatzort,
          am.attribute,
          am.kontaktDurchfuehrendeOrganisation,
          am.kontaktVermittelndeStelle
        ) AS `hash`,
        
        am.titel AS title,
        am.beschreibung AS description,
        
        JSON_EXTRACT_SCALAR(am.einsatzort, "$.ort") AS location,
        
        SAFE_CAST(JSON_EXTRACT_SCALAR(am.geoposition, "$.longitude") AS FLOAT64) AS longitude,
        SAFE_CAST(JSON_EXTRACT_SCALAR(am.geoposition, "$.latitude") AS FLOAT64) AS latitude,
        
        JSON_EXTRACT_SCALAR(am.durchfuehrendeOrganisation, "$.name") AS orga,
        
        CONCAT(
          JSON_EXTRACT_SCALAR(am.bild, "$.basisUrl"), 
          JSON_EXTRACT_SCALAR(am.bild, "$.jpg")
        ) AS image,
        NULL AS imageOrga,
        
        JSON_EXTRACT_SCALAR(am.durchfuehrendeOrganisation, "$.homepage") AS linkOrga,
        
        am.kontaktDurchfuehrendeOrganisation as contact,

        CONCAT(${local.aktion_mensch_base_url}, am.id) AS link,

        ARRAY(
          SELECT JSON_EXTRACT_SCALAR(attr, "$.name") 
          FROM UNNEST(JSON_EXTRACT_ARRAY(am.attribute)) attr 
          WHERE JSON_EXTRACT_SCALAR(attr, "$.typ") = 'schlagwort'
        ) AS hashTags,
        
        ARRAY(
          SELECT JSON_EXTRACT_SCALAR(attr, "$.name") 
          FROM UNNEST(JSON_EXTRACT_ARRAY(am.attribute)) attr
        ) AS uses,
        
        classifications.topics_v2,
        classifications.skills_v2, 
        classifications.topics_skills_embedding_v2,
        classifications.similarity_embedding,
        classifications.commitment_label,
        classifications.location_type,
        classifications.commitment_duration,
        classifications.commitment_duration_ai_reasoning,
        classifications.classification_failed,
        
        'Aktion Mensch' AS source,
        am._PARTITIONTIME AS partitiontime
      FROM
        `${local.datalake}.volunteering_aktion_mensch_raw` AS am
      JOIN 
        `${local.datalake}.classifications` AS classifications
      ON 
        am.id = classifications.id 
        AND classifications.hash = `${local.datalake}.calculate_aktion_mensch_hash`(
          am.titel,
          am.beschreibung,
          am.kurzbeschreibung,
          am.durchfuehrendeOrganisation,
          am.verortung,
          am.einsatzort,
          am.attribute,
          am.kontaktDurchfuehrendeOrganisation,
          am.kontaktVermittelndeStelle
        )
        AND classifications.type = 'aktion_mensch_volunteering'
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
  project    = local.project_id
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
