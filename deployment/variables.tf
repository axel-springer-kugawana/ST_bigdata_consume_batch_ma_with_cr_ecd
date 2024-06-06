variable "bucket" {
  description = "Bucket name"
  type        = string
  default     = "consume-batch-ma-with_cr_ecd"
}
variable "account_id" {
  description = "Identifier for the destination account"
  type        = string
  default     = ""
}

variable "env" {
  description = "Identifier for the environment"
  type        = string
  default     = ""
}

variable "table_name" {
  description = "Table name"
  type        = string
  default     = "offers_ma_geo"
}

variable "sns_email_address" {
  type    = string
  default = "o9v6i9t3h9e8j9w7@kugawana.slack.com"
}