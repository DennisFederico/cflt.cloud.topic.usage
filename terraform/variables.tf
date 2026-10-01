variable "project_id" {
  type        = string
  description = "The Google Cloud Project ID"
}

variable "region" {
  type        = string
  description = "The Google Cloud region to deploy resources in"
  default     = "europe-west2"
}

variable "zone" {
  type        = string
  description = "The Google Cloud zone to deploy resources in"
  default     = "europe-west2-a"
}

variable "network_name" {
  type        = string
  description = "The name of the VPC network to attach the VM to"
}

variable "subnet_name" {
  type        = string
  description = "The name of the subnet to attach the VM to"
  default     = "primary-subnet"
}

variable "machine_type" {
  type        = string
  description = "The Compute Engine machine type for the VM"
  default     = "e2-micro"
}

variable "vm_name" {
  type        = string
  description = "The name of the VM instance"
  default     = "cflt-topic-usage-dashboard"
}

variable "allowed_cidr_ranges" {
  type        = list(string)
  description = "List of CIDR ranges allowed to access the dashboard on port 8000"
  default     = ["0.0.0.0/0"]
}

variable "cflt_cloud_api_key" {
  type        = string
  description = "Confluent Cloud Telemetry API Key (defaults to value from local .env if empty)"
  default     = ""
  sensitive   = true
}

variable "cflt_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud Telemetry API Secret (defaults to value from local .env if empty)"
  default     = ""
  sensitive   = true
}

variable "allowed_ssh_cidr_ranges" {
  type        = list(string)
  description = "List of CIDR ranges allowed to SSH into the VM"
}
