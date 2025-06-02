variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "aws_profile" {
  description = "AWS CLI profile name"
  type        = string
  default     = "default"
}

variable "bucket_name" {
  description = "Raw data bucket name"
  type        = string
  default     = "people-data-raw-mk1p8t"
}
