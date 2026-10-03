variable "project_id" {
  type        = string
  description = "Target GCP project"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "env" {
  type        = string
  description = "Environment name - drives resource naming and labels"
  default     = "dev"
}
