provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}


resource "google_storage_bucket" "sample_data_bucket" {
  name                        = var.sample_data_bucket
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}


resource "google_storage_bucket" "iceberg_bucket" {
  name                        = var.iceberg_bucket
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}


#needs enabled https://console.developers.google.com/apis/api/biglake.googleapis.com/overview?project=pj-enhan-terraform-main
resource "google_biglake_iceberg_catalog" "catalog" {
  name         = var.iceberg_bucket
  catalog_type = "CATALOG_TYPE_GCS_BUCKET"
  depends_on   = [google_storage_bucket.iceberg_bucket]
}




resource "google_service_account" "dlt_sa" {
  account_id   = "dlt-pipeline"
  display_name = "dlt pipeline service account"
}

resource "google_project_iam_member" "dlt_sa_biglake_editor" {
  project = var.project
  role = "roles/biglake.editor"
  member = "serviceAccount:${google_service_account.dlt_sa.email}"
}

resource "google_storage_bucket_iam_member" "dlt_sa_iceberg_writer" {
  bucket = google_storage_bucket.iceberg_bucket.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.dlt_sa.email}"
}

resource "google_project_iam_member" "dlt_sa_storage" {
  project = var.project
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.dlt_sa.email}"
}

resource "google_project_iam_member" "dlt_sa_service_usage" {
  project = var.project
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = "serviceAccount:${google_service_account.dlt_sa.email}"
}

resource "google_service_account_key" "dlt_sa_key" {
  service_account_id = google_service_account.dlt_sa.name
}



# Outputs the key (base64 encoded)
output "dlt_sa_key" {
  value     = google_service_account_key.dlt_sa_key.private_key
  sensitive = true
}
