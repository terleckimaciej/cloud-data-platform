########################################
# modules/gcs/main.tf – GCS Data Lake #
########################################


resource "google_storage_bucket" "data_lake" {
name = var.bucket_name
location = "europe-central2"
project = var.project_id


storage_class = "STANDARD"
force_destroy = true


uniform_bucket_level_access = true
}


# IAM permissions for components that access GCS
resource "google_storage_bucket_iam_member" "cloud_run_access" {
bucket = google_storage_bucket.data_lake.name
role = "roles/storage.objectAdmin"
member = "serviceAccount:${var.cloud_run_sa}"
}


resource "google_storage_bucket_iam_member" "composer_access" {
bucket = google_storage_bucket.data_lake.name
role = "roles/storage.objectAdmin"
member = "serviceAccount:${var.composer_sa}"
}


resource "google_storage_bucket_iam_member" "dataproc_access" {
bucket = google_storage_bucket.data_lake.name
role = "roles/storage.objectViewer"
member = "serviceAccount:${var.dataproc_sa}"
}


resource "google_storage_bucket_object" "pyspark_curated_transform" {
  name   = "pyspark_jobs/curated_transform.py"  # Final GCS path
  bucket = google_storage_bucket.data_lake.name
  source = "${path.module}/../../pyspark_jobs/spark-transform-job.py"  # Lokalna ścieżka w repo
}
