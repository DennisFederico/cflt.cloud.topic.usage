output "vm_external_ip" {
  value       = google_compute_instance.vm.network_interface[0].access_config[0].nat_ip
  description = "The public external IP address of the Compute Engine VM."
}

output "dashboard_url" {
  value       = "http://${google_compute_instance.vm.network_interface[0].access_config[0].nat_ip}:8000"
  description = "The HTTP URL to access the Confluent Cloud Topic Usage Dashboard Web UI."
}

output "deploy_bucket_name" {
  value       = google_storage_bucket.deploy_bucket.name
  description = "The GCS bucket where deployment packages are stored."
}

output "vm_name" {
  value       = google_compute_instance.vm.name
  description = "The name of the Compute Engine VM."
}

