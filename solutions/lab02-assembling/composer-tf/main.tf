// Enable the Cloud Composer API
resource "google_project_service" "composer_api" {
  project = var.project_id
  service = "composer.googleapis.com"
  // Disabling the API while environments are running
  // can seriously break stuff. Remove the below once
  // all environments are deleted.
  disable_on_destroy = false
}

// Create the custom service account for the nodes
resource "google_service_account" "composer_account" {
  account_id   = "composer-account"
  display_name = "Custom SA for Cloud Composer Nodes"
}
// Bind the new service account to the composer.worker role
resource "google_project_iam_member" "composer_account_worker_binding" {
  project  = var.project_id
  member   = "serviceAccount:${google_service_account.composer_account.email}"
  role     = "roles/composer.worker"
}

// Lab 2 adding the GCS admin role
resource "google_project_iam_member" "composer_account_gcs_admin" {
  project  = var.project_id
  member   = "serviceAccount:${google_service_account.composer_account.email}"
  role     = "roles/storage.admin"
}

// Create the Composer instance
resource "google_composer_environment" "lab_environment" {
  name    = "lab-environment"
  project = var.project_id
  region  = var.default_region

  config {
    software_config {
      image_version = "composer-3-airflow-2.9.1"
    }
    node_config {
      service_account = google_service_account.composer_account.email
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"
  } // ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE
}