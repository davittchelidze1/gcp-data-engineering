terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  backend "gcs" {
    bucket = "gcp-learning-507108-tfstate"
    prefix = "day14"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "us-central1"
}

locals {
  labels = {
    platform   = "orders"
    managed_by = "terraform"
    day        = "14"
  }
}

# ---------------- bronze: raw landing ----------------
resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_id}-bronze"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ---------------- silver: curated tables ----------------
resource "google_storage_bucket" "silver" {
  name                        = "${var.project_id}-silver"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels
}

resource "google_bigquery_dataset" "platform" {
  dataset_id                 = "platform"
  location                   = var.region
  description                = "Day 14 end-to-end orders platform"
  labels                     = local.labels
  delete_contents_on_destroy = true
}

# ---------------- gold: the table analysts query ----------------
resource "google_bigquery_table" "daily_revenue" {
  dataset_id          = google_bigquery_dataset.platform.dataset_id
  table_id            = "gold_daily_revenue"
  deletion_protection = false
  labels              = local.labels

  time_partitioning {
    type  = "DAY"
    field = "order_date"
  }
  clustering = ["country"]

  schema = jsonencode([
    { name = "order_date", type = "DATE", mode = "REQUIRED" },
    { name = "country", type = "STRING", mode = "REQUIRED" },
    { name = "orders", type = "INT64" },
    { name = "revenue", type = "NUMERIC" },
    { name = "avg_order_value", type = "NUMERIC" },
    { name = "built_at", type = "TIMESTAMP" },
  ])
}

# ---------------- the pipeline identity ----------------
resource "google_service_account" "platform" {
  account_id   = "orders-platform"
  display_name = "Day 14 orders platform pipeline"
}

resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.platform.email}"
}

resource "google_bigquery_dataset_iam_member" "editor" {
  dataset_id = google_bigquery_dataset.platform.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.platform.email}"
}

resource "google_storage_bucket_iam_member" "bronze_read" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.platform.email}"
}

resource "google_storage_bucket_iam_member" "silver_write" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.platform.email}"
}

output "bronze_bucket" { value = google_storage_bucket.bronze.name }
output "silver_bucket" { value = google_storage_bucket.silver.name }
output "dataset" { value = google_bigquery_dataset.platform.dataset_id }
output "pipeline_sa" { value = google_service_account.platform.email }
