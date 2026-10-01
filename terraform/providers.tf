terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  credentials = file("${path.module}/../.gcloud.json")
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}
