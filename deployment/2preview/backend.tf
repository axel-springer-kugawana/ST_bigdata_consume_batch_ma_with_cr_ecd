terraform {
  backend "s3" {
    bucket  = "st-terraform-state-storage-preview"
    encrypt = true
    key     = "consume-batch-ma-with-cr-ecd.tfstate"
    region  = "eu-central-1"
    profile = "iwt-bigdata-preview"
  }
}