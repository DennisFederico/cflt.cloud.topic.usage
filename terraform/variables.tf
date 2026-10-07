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
  description = "Confluent Cloud Telemetry API Key (legacy single-org fallback)"
  default     = ""
  sensitive   = true
}

variable "cflt_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud Telemetry API Secret (legacy single-org fallback)"
  default     = ""
  sensitive   = true
}

variable "confluent_orgs" {
  type = list(object({
    id         = string
    name       = string
    api_key    = string
    api_secret = string
  }))
  description = "List of Confluent Cloud organizations (id, name, api_key, api_secret) for multi-org telemetry"
  default     = []
  sensitive   = true
}

variable "override_all_orgs_on_deploy" {
  type        = bool
  description = "If true, deployment enforces Terraform orgs as sole source of truth and removes GUI-only orgs. If false (default), merges deployment orgs and preserves GUI-added orgs."
  default     = false
}

variable "prom_scrape_interval" {
  type        = string
  description = "Prometheus scrape interval for metrics export"
  default     = "1m"
}

variable "prom_scrape_timeout" {
  type        = string
  description = "Prometheus scrape timeout"
  default     = "55s"
}

variable "prom_retention_time" {
  type        = string
  description = "Prometheus historical TSDB retention time"
  default     = "180d"
}

variable "allowed_ssh_cidr_ranges" {
  type        = list(string)
  description = "List of CIDR ranges allowed to SSH into the VM"
}
