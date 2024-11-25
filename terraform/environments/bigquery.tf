locals {
  dataset_id_datalake = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datalake_development
  dataset_id_datapool = local.environment == "production" ? data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_production : data.terraform_remote_state.holi_infra_state.outputs.bigquery_dataset_id_datapool_development

  datalake = "${local.default_project}.${local.dataset_id_datalake}"
  datapool = "${local.default_project}.${local.dataset_id_datapool}"




}

resource "google_bigquery_table" "volunteering_voltastics_deduped_aggregated_images" {
  project    = local.default_project
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw_deduped_aggregated_images"

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        ANY_VALUE(longitude) as longitude,
        ANY_VALUE(imageOrga) as imageOrga,
        location,
        title,
        source,
        ANY_VALUE(hashTags) as hashTags,
        description,
        array_agg(distinct(image)) as images,
        link,
        ANY_VALUE(orga) as orga,
        MIN(id) as id,
        ANY_VALUE(latitude) as latitude,
        ANY_VALUE(uses) as uses,
        cast(FARM_FINGERPRINT(CONCAT(title, '|', description, '|', location, '|', link)) as string) as `hash`,
        _PARTITIONTIME as date
      FROM
        `holi-shared.datalake_production.volunteering_voltastics_raw`
      GROUP BY title, description, location, link, source, date
   SQL
  }
}

resource "google_bigquery_table" "volunteering_voltastics_deduped" {
  project    = local.default_project
  dataset_id = local.dataset_id_datalake
  table_id   = "volunteering_voltastics_raw_deduped"

  view {
    use_legacy_sql = false
    query          = <<SQL
      SELECT
        longitude,
        imageOrga,
        location,
        title,
        source,
        hashTags,
        description,
        images[SAFE_OFFSET(0)] as image,
        link,
        orga,
        id,
        latitude,
        uses,
        `hash`,
        date
      FROM
        ${local.datalake}.${google_bigquery_table.volunteering_voltastics_deduped_aggregated_images.table_id}
   SQL
  }
}
