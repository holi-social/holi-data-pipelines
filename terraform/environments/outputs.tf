output "bigquery_table_name_classifications" {
  value = "${local.datalake}.${google_bigquery_table.classifications.table_id}"
}
