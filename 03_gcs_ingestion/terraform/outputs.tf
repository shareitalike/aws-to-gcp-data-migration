output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "validated_bucket" {
  value = google_storage_bucket.validated.name
}

output "quarantine_bucket" {
  value = google_storage_bucket.quarantine.name
}

output "processed_bucket" {
  value = google_storage_bucket.processed.name
}

output "staging_dataset" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}
