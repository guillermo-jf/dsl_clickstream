# Data Source: Google Compute Default Service Account
# Fetches information about the default service account for Compute Engine.
# This makes the configuration more portable and less error-prone.
data "google_compute_default_service_account" "default" {}

# Resource: Google Compute Instance Template
# This defines a reusable template for creating Compute Engine VMs.
resource "google_compute_instance_template" "vm_template_clickstream" {
  # Unique name for the instance template
  name_prefix = "vm-dsl-template-"
  description = "Standard VM configuration for streaming events analytics"

  # Machine type defines the virtual hardware for the VM.
  # 'e2-medium' is a good balance of cost and performance for many workloads.
  machine_type = "e2-medium"

  # Disk configuration for the VM
  disk {
    # Type of disk (e.g., pd-standard, pd-balanced, pd-ssd)
    # 'pd-standard' is a cost-effective option.
    disk_type    = "pd-standard"
    # Size of the boot disk in GB
    disk_size_gb = 20
    # Defines the source image for the boot disk.
    # We use 'debian-cloud/debian-11' for a common Linux distribution.
    source_image = "debian-cloud/debian-11"
    # Set to true to make this the boot disk
    auto_delete  = true
    boot         = true
  }

  # Network interface configuration
  network_interface {
    # The name of the VPC network to attach to.
    # 'default' refers to the default VPC network in your project.
    network = "default"

    # Access configurations define external IP addresses.
    # Setting an access_config with no parameters assigns an ephemeral public IP.
    access_config {}
  }

  # Startup script to be executed when the VM instance starts.
  # This script sets up the Python environment and installs necessary libraries.
  metadata_startup_script = <<-EOT
    #!/bin/bash

    # This script is designed to be used as a startup script for a Google Cloud Compute Engine VM.
    # It automates the setup of a Python environment, including a virtual environment
    # and necessary libraries for Google Cloud services.
    # This script no longer assumes or references a specific Python script file.

    # --- Configuration ---
    TARGET_DIR="dsl_pull_etl" # Relative path, as we'll 'cd' into it

    # Ensure non-interactive mode for apt-get to prevent prompts during installation
    export DEBIAN_FRONTEND=noninteractive

    echo "--- Starting VM Startup Script ---"

    # 1. Update apt package lists
    # This command refreshes the list of available packages and their versions from the repositories.
    echo "Updating apt package lists..."
    sudo apt update -y
    if [ $? -eq 0 ]; then
        echo "Apt package lists updated successfully."
    else
        echo "Error updating apt package lists. Script will continue, but some installations might fail."
    fi

    # 2. Install Python 3, pip, and python3-venv (if not already present)
    # These are essential for creating and managing Python virtual environments.
    echo "Installing Python 3, pip, and python3-venv..."
    sudo apt install python3 python3-pip -y
    sudo apt-get install python3-venv -y
    if [ $? -eq 0 ]; then
        echo "Python 3, pip, and python3-venv installed successfully."
    else
        echo "Error installing Python 3, pip, and python3-venv. Continuing anyway, but virtual environment setup might fail."
    fi

    # 3. Create the target directory
    # This directory will house the virtual environment.
    # The $$ escapes the dollar sign for Terraform's parser.
    echo "Checking and creating target directory: /$${TARGET_DIR}..."
    mkdir -p /$${TARGET_DIR}
    if [ $? -eq 0 ]; then
        echo "Directory '/$${TARGET_DIR}' created successfully."
    else
        echo "Error creating directory '/$${TARGET_DIR}'. It might already exist. Continuing."
    fi

    # 4. Change into the target directory
    # All subsequent operations (venv creation) will happen within this directory.
    echo "Changing directory to /$${TARGET_DIR}..."
    cd /$${TARGET_DIR}
    if [ $? -eq 0 ]; then
        echo "Successfully changed directory to /$${TARGET_DIR}."
    else
        echo "Error changing directory to /$${TARGET_DIR}. Exiting."
        exit 1 # Critical failure, cannot proceed without being in the correct directory.
    fi

    # 5. Create a virtual environment named 'visits_env'
    echo "Creating virtual environment 'visits_env'..."
    python3 -m venv visits_env
    if [ $? -eq 0 ]; then
        echo "Virtual environment 'visits_env' created successfully."
    else
        echo "Error creating virtual environment 'visits_env'. Exiting."
        exit 1 # Critical failure, cannot proceed with package installation.
    fi

    # 6. Activate the virtual environment
    # Note: 'source' command is used for activating virtual environments.
    echo "Activating virtual environment 'visits_env'..."
    source visits_env/bin/activate
    if [ $? -eq 0 ]; then
        echo "Virtual environment 'visits_env' activated."
    else
        echo "Error activating virtual environment 'visits_env'. Continuing anyway, but pip installs might go global."
    fi

    # 7. Install Python packages within the virtual environment
    echo "Installing Python packages..."
    pip install google-cloud-storage
    pip install google-cloud-pubsub
    pip install google-cloud-bigquery
    pip install google-cloud-bigquery-storage
    pip install Flask
    if [ $? -eq 0 ]; then
        echo "Python packages installed successfully."
    else
        echo "Error installing Python packages. Check network connectivity or package names."
    fi

    # 8. Download the Python script from GCS
    # The gsutil command-line tool is pre-installed on standard Google Cloud images.
    echo "Downloading Python script from GCS..."
    gsutil cp gs://jellyfish-training-demo-6/dsl-project/pull_subscriber_BQ_Storage_API.py .
    if [ $? -eq 0 ]; then
        echo "Python script downloaded successfully."
    else
        echo "Error downloading Python script from GCS. Check bucket/object path and permissions."
    fi


    # 9. Set gcloud project ID (optional, but good practice)
    echo "Setting gcloud project ID..."
    gcloud config set project jellyfish-training-demo-6
    if [ $? -eq 0 ]; then
        echo "gcloud project set."
    else
        echo "Error setting gcloud project. 'gcloud' might not be installed or authenticated."
    fi

    # 10. Set gcloud preferred region (optional)
    echo "Setting gcloud compute region..."
    gcloud config set compute/region us-central1
    if [ $? -eq 0 ]; then
        echo "gcloud compute region set."
    else
        echo "Error setting gcloud compute region."
    fi

    echo "--- VM Startup Script Finished ---"
    EOT

  # Service account for the VM instance.
  # This grants the VM permissions to access other GCP services.
  service_account {
    email  = data.google_compute_default_service_account.default.email
    scopes = ["cloud-platform"] # Grant broad access for demonstration. Adjust scopes for production.
  }

  # Tags for the instance template.
  tags = ["clickstream", "dsl", "standard-vm"]

  can_ip_forward = false

  # Optional: Labels for the instance template for organization.
  labels = {
    environment = "dev"
    purpose     = "dsl_etl_pipeline"
  }
}

# Resource: Google Compute Instance Group Manager (Managed Instance Group)
resource "google_compute_instance_group_manager" "app_instance_group" {
  name                 = "app-instance-group"
  description          = "Managed Instance Group for the standard VM application"
  zone                 = "us-central1-b" # Specify a zone. Consider multiple zones for high availability.
  base_instance_name   = "dsl-clickstream" # Base name for instances created in the group

  # The version block is required to specify the instance template
  version {
    instance_template = google_compute_instance_template.vm_template_clickstream.id
  }

  target_size          = 1 # Initial number of instances in the group
}

# Resource: Google Compute Autoscaler
resource "google_compute_autoscaler" "app_autoscaler" {
  name   = "app-autoscaler"
  zone   = google_compute_instance_group_manager.app_instance_group.zone
  target = google_compute_instance_group_manager.app_instance_group.id

  # Autoscaling policy definition
  autoscaling_policy {
    min_replicas = 1 # Minimum number of instances
    max_replicas = 5 # Maximum number of instances

    # Scale based on CPU utilization
    cpu_utilization {
      target = 0.6 # Target 60% CPU utilization
    }
  }
}

# --- OUTPUTS ---

output "instance_template_name" {
  description = "The name of the created Compute Engine Instance Template."
  value       = google_compute_instance_template.vm_template_clickstream.name
}

output "instance_group_name" {
  description = "The name of the created Managed Instance Group."
  value       = google_compute_instance_group_manager.app_instance_group.name
}

output "autoscaler_name" {
  description = "The name of the created Autoscaler."
  value       = google_compute_autoscaler.app_autoscaler.name
}
