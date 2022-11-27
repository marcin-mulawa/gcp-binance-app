variable "project_id" {
  description = "The project ID"
  type        = string
  default     = "binance-app-gcp-369914"
}

variable "topic_name" {
  description = "The name of the topic"
  type        = string
  default     = "binance"
}

variable "subscription_name" {
  description = "The name of the subscription"
  type        = string
  default     = "binance-sub"
}

variable "dataset_name" {
  description = "The name of the dataset"
  type        = string
  default     = "binance"
}

variable "dataset_location" {
  description = "The location of the dataset"
  type        = string
  default     = "eu"
}

variable "table_name" {
  description = "The name of the table"
  type        = string
  default     = "binance_klines"
}

variable "bucket_name" {
  description = "The name of the bucket"
  type        = string
  default     = "binance-app-gcp-369914-bucket"
}

variable "bucket_location" {
  description = "The location of the bucket"
  type        = string
  default     = "EU"
}
