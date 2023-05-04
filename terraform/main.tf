terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
    digitalocean = {
      source = "digitalocean/digitalocean"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  credentials = file(var.credentials)
}

provider "digitalocean" {
  token = var.do_token
}

# Virtual machine
resource "digitalocean_droplet" "idealista_vm" {
  name   = "idealista-pipeline-vm"
  size   = var.do_machine_type
  image  = var.do_vm_image
  region = var.do_region

  ssh_keys = [
    digitalocean_ssh_key.idealista_vm_ssh_key.id
  ]
}

resource "digitalocean_ssh_key" "idealista_vm_ssh_key" {
  name       = "idealista_vm_ssh_key"
  public_key = file(var.vm_ssh_pub_key)
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-idealista" {
  name          = "${var.data_lake_bucket}_${var.project_id}" 
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# Data Warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
}
