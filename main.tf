terraform {
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

variable "project_id" {
  description = "ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "Región de GCP"
  type        = string
  default     = "US"
}

# Bucket de Cloud Storage para la capa Bronze (datos crudos)
resource "google_storage_bucket" "bronze_landing" {
  name     = "${var.project_id}-bronze-landing"
  location = var.region

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Dataset raw (Bronze)
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  project     = var.project_id
  location    = var.region
  description = "Capa Bronze - datos crudos sin procesar"
}

# Dataset silver
resource "google_bigquery_dataset" "silver" {
  dataset_id  = "silver"
  project     = var.project_id
  location    = var.region
  description = "Capa Silver - datos limpios y particionados"
}

# Dataset gold
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "gold"
  project     = var.project_id
  location    = var.region
  description = "Capa Gold - datos agregados listos para negocio"
}