terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

resource "aws_s3_bucket" "data_bucket" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = {
    Name        = "Data Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "raw_data" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "temp/"
  content = ""
}

