terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

#enable pubsub api
resource "google_project_service" "pubsub" {
  project = var.project_id
  service = "pubsub.googleapis.com"
}

#enable dataflow api
resource "google_project_service" "dataflow" {
  project = var.project_id
  service = "dataflow.googleapis.com"
}

#enable bigquery api
resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"
}

#create pubsub topic
resource "google_pubsub_topic" "topic" {
  name = var.topic_name
}

#create pubsub subscription
resource "google_pubsub_subscription" "subscription" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.name
}

#create bigquery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_name
  friendly_name               = var.dataset_name
  location                    = var.dataset_location
#   default_table_expiration_ms = 3600000
}

#create bigquery table
resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.table_name
  schema     = file("schema.json")
}



