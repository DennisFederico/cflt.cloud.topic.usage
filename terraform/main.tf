# Locals for dynamic credential parsing
locals {
  # Parse local .env file in the root directory if it exists
  env_content = fileexists("${path.module}/../.env") ? file("${path.module}/../.env") : ""
  
  # Extract values using regex matching
  extracted_key_list = regexall("(?m)^CLOUD_API_KEY\\s*=\\s*\"?([^\r\n\"]+)\"?", local.env_content)
  extracted_sec_list = regexall("(?m)^CLOUD_API_SECRET\\s*=\\s*\"?([^\r\n\"]+)\"?", local.env_content)
  
  extracted_key = length(local.extracted_key_list) > 0 ? local.extracted_key_list[0][0] : ""
  extracted_sec = length(local.extracted_sec_list) > 0 ? local.extracted_sec_list[0][0] : ""

  # If variables are provided, use them. Otherwise fall back to extracted values from .env.
  final_cflt_key    = var.cflt_cloud_api_key != "" ? var.cflt_cloud_api_key : local.extracted_key
  final_cflt_secret = var.cflt_cloud_api_secret != "" ? var.cflt_cloud_api_secret : local.extracted_sec

  # Compute deterministic hash of application source files to detect code changes
  app_files = [
    for f in setunion(
      fileset("${path.module}/..", "app/**"),
      fileset("${path.module}/..", "docker-compose.yml")
    ) : f
    if !strcontains(f, ".venv") &&
       !strcontains(f, "__pycache__") &&
       !strcontains(f, ".DS_Store") &&
       !strcontains(f, "app/clear")
  ]

  app_source_hash = sha256(join("", [
    for f in local.app_files : filesha256("${path.module}/../${f}")
  ]))
}

# Data sources for network configuration
data "google_compute_network" "vpc" {
  name    = var.network_name
  project = var.project_id
}

data "google_compute_subnetwork" "subnet" {
  name    = var.subnet_name
  region  = var.region
  project = var.project_id
}

# Create a zip of the local files
resource "null_resource" "build_zip" {
  triggers = {
    app_hash = local.app_source_hash
  }

  provisioner "local-exec" {
    working_dir = "${path.module}/.."
    command     = "rm -f terraform/app_deploy.zip && zip -r terraform/app_deploy.zip app docker-compose.yml -x \"app/.venv/*\" \"app/__pycache__/*\" \"app/.DS_Store\" \"app/clear/*\" \"app/**/.venv/*\" \"app/**/__pycache__/*\" \"app/**/.DS_Store\" \"app/**/clear/*\""
  }
}

# Generate unique bucket name suffix
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# GCS Bucket for deployment artifacts
resource "google_storage_bucket" "deploy_bucket" {
  name                        = "cflt-usage-dashboard-${random_id.bucket_suffix.hex}"
  location                    = var.region
  project                     = var.project_id
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 14
    }
  }
}

# Upload ZIP deployment package to GCS
resource "google_storage_bucket_object" "app_archive" {
  name   = "app_deploy-${local.app_source_hash}.zip"
  bucket = google_storage_bucket.deploy_bucket.name
  source = "${path.module}/app_deploy.zip"

  depends_on = [null_resource.build_zip]
}

# Service Account for the VM
resource "google_service_account" "vm_sa" {
  create_ignore_already_exists = true
  account_id   = "cflt-dashboard-vm-sa"
  display_name = "Service Account for CFLT Usage Dashboard VM"
  project      = var.project_id
}

# IAM permissions to allow VM Service Account to read from deployment bucket
resource "google_storage_bucket_iam_member" "viewer" {
  bucket = google_storage_bucket.deploy_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.vm_sa.email}"
}

# Firewall Rule to allow HTTP traffic on port 8000
resource "google_compute_firewall" "allow_http_8000" {
  name    = "allow-cflt-dashboard-8000-${var.vm_name}"
  network = data.google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["8000"]
  }

  source_ranges = var.allowed_cidr_ranges
  target_tags   = [var.vm_name]
}

# Compute Engine Instance
resource "google_compute_instance" "vm" {
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone
  project      = var.project_id

  tags = [var.vm_name]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.subnet.self_link
    access_config {
      // Ephemeral public IP to allow dashboard access
    }
  }

  service_account {
    email  = google_service_account.vm_sa.email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    bucket_name      = google_storage_bucket.deploy_bucket.name
    archive_name     = google_storage_bucket_object.app_archive.name
    cloud_api_key    = local.final_cflt_key
    cloud_api_secret = local.final_cflt_secret
  })

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [metadata_startup_script]
  }

  depends_on = [google_storage_bucket_object.app_archive]
}

# In-place redeployment on running VM without recreating the instance or wiping Prometheus data
resource "null_resource" "redeploy_app" {
  triggers = {
    app_hash = local.app_source_hash
  }

  depends_on = [
    google_storage_bucket_object.app_archive,
    google_compute_instance.vm
  ]

  provisioner "local-exec" {
    command = <<-EOT
      gcloud compute ssh ${var.vm_name} --zone ${var.zone} --project ${var.project_id} --command "
        sudo gsutil cp gs://${google_storage_bucket.deploy_bucket.name}/${google_storage_bucket_object.app_archive.name} /tmp/app_deploy.zip && \
        sudo unzip -o /tmp/app_deploy.zip -d /opt/cflt-app && \
        cd /opt/cflt-app && \
        sudo docker compose up --build -d
      "
    EOT
  }
}

# Firewall Rule to allow SSH traffic on port 22
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh-cflt-dashboard-${var.vm_name}"
  network = data.google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.allowed_ssh_cidr_ranges
  target_tags   = [var.vm_name]
}
