# Configure the Google Cloud provider
provider "google" {
  project = "jellyfish-training-demo-6" # Project ID as requested
  region  = "us-central1"               # Consider changing to your desired region
}

# --- Resource: Google Cloud Composer Environment ---
# This resource creates a Cloud Composer environment, which is a managed Apache Airflow service.
resource "google_cloud_composer_environment" "composer_env" {
  name    = "dsl-project-clickstream" # Name for your Composer environment
  region  = var.region        # Uses the region defined in the provider

  # Environment configuration
  config {
    node_count = 3 # Number of GKE cluster nodes for your Composer environment (min 3 for Composer 2)
    node_config {
      machine_type = "e2-medium" # Machine type for the GKE nodes
      # The service account used by the Composer environment's GKE cluster nodes.
      # It's recommended to create a dedicated service account with granular permissions
      # for production environments. This uses the default Compute Engine service account.
      service_account = "jellyfish-training-demo-6@appspot.gserviceaccount.com" # REPLACE with your project's default Compute Engine service account email
      # Network and subnetwork details can be added here if you're not using default VPC
      # network    = "default"
      # subnetwork = "default"
    }
    # Software configuration for Airflow
    software_config {
      image_version = "composer-2.2.3-airflow-2.4.3" # Specify a Composer image version
      # Example: Override Airflow configuration (uncomment and modify as needed)
      # airflow_config_overrides = {
      #   "webserver-web_server_worker_timeout" = "120"
      # }
      # Example: Install Python packages (uncomment and modify as needed)
      # pypi_packages = {
      #   "apache-airflow-providers-google" = ""
      #   "pandas"                          = ""
      # }
    }
    # Web server network access control
    web_server_network_access_control {
      allowed_ip_ranges {
        value       = "0.0.0.0/0" # WARNING: This allows access from anywhere. Restrict for production.
        description = "Allow access to Composer UI from all IP ranges (for demonstration)"
      }
    }
    # Private IP setup (recommended for production, uncomment if needed)
    # private_environment_config {
    #   enable_private_endpoint = false
    #   enable_private_builds   = false
    # }
  }

  # Labels for the Composer environment for organization and cost tracking
  labels = {
    environment = "demo"
    purpose     = "data-orchestration"
  }

  # Optional: Grant roles to the Composer service account (replace 'your-project-id' and 'your-composer-service-account')
  # This part is generally handled by the `google_cloud_composer_environment` resource creation
  # but if you need additional roles, you would define them here or separately.
}

# --- Output ---
# This output will display the name of the created Cloud Composer environment
output "composer_environment_name" {
  description = "The name of the created Cloud Composer environment."
  value       = google_cloud_composer_environment.composer_env.name