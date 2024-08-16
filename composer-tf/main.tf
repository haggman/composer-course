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
module "composer_service_account" {
  source  = "terraform-google-modules/service-accounts/google"
  version = "~> 4.0"

  project_id = var.project_id
  names      = ["composer-account"]
  // Add roles as needed
  project_roles = ["${var.project_id}=>roles/composer.worker", "${var.project_id}=>roles/bigquery.admin"]
  display_name  = "Composer Account"
}

// Create the Composer instance
resource "google_composer_environment" "lab_environment" {
  name    = "lab-environment"
  project = var.project_id
  region  = var.default_region

  config {
    software_config {
      image_version = "composer-3-airflow-2.7.3-build.11"
    }
    node_config {
      service_account = module.composer_service_account.email
    }

    workloads_config {
      worker { // scheduler, triggerer, web_server
        min_count  = 2
        max_count  = 6
        cpu        = 1
        memory_gb  = 2
        storage_gb = 2
      }
    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
  } // ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE
}