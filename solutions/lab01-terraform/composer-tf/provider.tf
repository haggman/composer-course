// At the time of this writing, you'll need 
// beta support to leverage Composer 3
provider "google-beta" {
  project = var.project_id
  region  = var.default_region
}