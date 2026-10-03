locals {
  prefix = "${var.project_id}-${var.env}"
  labels = {
    env        = var.env
    managed_by = "terraform"
    day        = "13"
  }
}

# ---------- storage ----------
resource "google_storage_bucket" "landing" {
  name                        = "${local.prefix}-landing"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ---------- warehouse ----------
resource "google_bigquery_dataset" "warehouse" {
  dataset_id                 = "${var.env}_warehouse"
  location                   = var.region
  description                = "Managed by Terraform - Day 13"
  labels                     = local.labels
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "orders" {
  dataset_id          = google_bigquery_dataset.warehouse.dataset_id
  table_id            = "orders"
  deletion_protection = false
  labels              = local.labels

  time_partitioning {
    type                     = "DAY"
    field                    = "order_ts"
    require_partition_filter = true
  }
  clustering = ["customer_id"]

  schema = jsonencode([
    { name = "order_id", type = "INT64", mode = "REQUIRED" },
    { name = "customer_id", type = "INT64", mode = "REQUIRED" },
    { name = "order_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "amount", type = "NUMERIC", mode = "NULLABLE" },
  ])
}

# ---------- workload identity ----------
resource "google_service_account" "pipeline" {
  account_id   = "${var.env}-pipeline"
  display_name = "${var.env} pipeline runner (Terraform managed)"
}

# least privilege: can run jobs, read the bucket, write only its own dataset
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_reader" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}
