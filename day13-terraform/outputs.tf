output "landing_bucket" { value = google_storage_bucket.landing.url }
output "dataset" { value = google_bigquery_dataset.warehouse.dataset_id }
output "partitioned_table" { value = google_bigquery_table.orders.id }
output "pipeline_sa" { value = google_service_account.pipeline.email }
