variable "project_id" {
  description = "GCP project ID where Cloud SQL is provisioned."
  type        = string
}

variable "region" {
  description = "GCP region for the Cloud SQL instance."
  type        = string
  default     = "asia-northeast3"
}

variable "network_name" {
  description = "Dedicated VPC network name for GAPGUARD."
  type        = string
  default     = "gapguard-vpc"
}

variable "subnet_name" {
  description = "Subnet name for GAPGUARD workloads."
  type        = string
  default     = "gapguard-subnet"
}

variable "subnet_cidr" {
  description = "Private CIDR for the GAPGUARD workload subnet."
  type        = string
  default     = "10.10.0.0/24"
}

variable "private_services_prefix_length" {
  description = "CIDR prefix length reserved for Cloud SQL private services access."
  type        = number
  default     = 16
}

variable "db_name" {
  description = "Cloud SQL instance and PostgreSQL database name prefix."
  type        = string
}

variable "db_user" {
  description = "PostgreSQL user to create."
  type        = string
}

variable "db_password" {
  description = "Password for the PostgreSQL user."
  type        = string
  sensitive   = true
}

variable "machine_tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-custom-1-3840"
}

variable "deletion_protection" {
  description = "Whether Terraform should protect the Cloud SQL instance from deletion."
  type        = bool
  default     = true
}
