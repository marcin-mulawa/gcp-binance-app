variable "project_id" {
  description = "The project ID "
  type = string
}

variable "topic_name" {
  description = "The name of the topic"
  type = string
  default = "binance"
}

variable "subscription_name" {
  description = "The name of the subscription"
  type = string
  default = "binance-sub"
}

variable "dataset_name" {
  description = "The name of the dataset"
  type = string
  default = "binance"
}

variable "dataset_location" {
  description = "The location of the dataset"
  type = string
  default = "eu"
}

variable "table_name" {
  description = "The name of the table"
  type = string
  default = "binance_klines"
}

variable "table_schema" {
  description = "The schema of the table"
  type = list(object({
    name = string
    type = string
    mode = string
  }))
  default = [
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"  
    },
    {  
      name = "interval"
      type = "STRING"
      mode = "REQUIRED" 
    },
    {
      name = "open_time"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "close_time"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "open"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "high"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "low"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "close"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "volume"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "quote_asset_volume"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "number_of_trades"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "taker_buy_base_asset_volume"
      type = "NUMERIC"
      mode = "REQUIRED"
    },
    {
      name = "taker_buy_quote_asset_volume"
      type = "NUMERIC"
      mode = "REQUIRED"
    }
  ]
}