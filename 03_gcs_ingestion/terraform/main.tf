# ──────────────────────────────────────────────────────────
#  GCS Buckets + BigQuery Dataset/Tables for Migration
#  Free Tier Compatible
# ──────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GCS BUCKETS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Raw landing zone — files arrive here from S3
resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-data-lake-raw"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true # free-tier: easy cleanup

  uniform_bucket_level_access = true

  versioning {
    enabled = false # save cost on free tier
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "demo"
    pipeline    = "aws-gcp-migration"
  }
}

# Validated files — passed schema/quality checks
resource "google_storage_bucket" "validated" {
  name          = "${var.project_id}-data-lake-validated"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  labels = {
    environment = "demo"
    pipeline    = "aws-gcp-migration"
  }
}

# Quarantine — files that failed validation
resource "google_storage_bucket" "quarantine" {
  name          = "${var.project_id}-data-lake-quarantine"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 14
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "demo"
    pipeline    = "aws-gcp-migration"
  }
}

# Processed — Spark output, ready for BigQuery
resource "google_storage_bucket" "processed" {
  name          = "${var.project_id}-data-lake-processed"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  labels = {
    environment = "demo"
    pipeline    = "aws-gcp-migration"
  }
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BIGQUERY DATASETS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging Dataset"
  description   = "Temporary staging area for pipeline loads"
  location      = "US"

  # Auto-delete tables after 7 days (cost safety)
  default_table_expiration_ms = 604800000

  labels = {
    environment = "demo"
  }
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id    = "analytics"
  friendly_name = "Analytics Dataset"
  description   = "Production analytics tables"
  location      = "US"

  labels = {
    environment = "demo"
  }
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BIGQUERY TABLES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

resource "google_bigquery_table" "staging_orders" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "enriched_orders_staging"
  deletion_protection = false

  schema = file("${path.module}/schemas/enriched_orders.json")
}

resource "google_bigquery_table" "production_orders" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "enriched_orders"
  deletion_protection = false

  time_partitioning {
    type                     = "DAY"
    field                    = "process_date"
    require_partition_filter = true
  }

  clustering = ["user_id", "event_type"]

  schema = file("${path.module}/schemas/enriched_orders.json")

  labels = {
    pipeline = "aws-gcp-migration"
  }
}
