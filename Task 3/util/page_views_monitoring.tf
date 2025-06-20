# Configure the Google Cloud provider
provider "google" {
  project = "jellyfish-training-demo-6" # REPLACE WITH YOUR GCP PROJECT ID
  region  = "us-central1"         # REPLACE WITH YOUR DESIRED REGION
  subscription = "clickstream_pull"
}

# Variable for the email address to send alerts to
# IMPORTANT: Replace "your-email@example.com" with your actual email.
variable "alert_email_address" {
  description = "Email address to receive alert notifications"
  type        = string
  default     = "guillermo.perasso@jellyfish.com" # <--- REPLACE THIS WITH YOUR EMAIL ADDRESS
}

# --- Resource 1: Google Cloud Logging Metric for Page Views ---
# This resource creates a custom log-based metric that counts "page views".
# It filters log entries originating from Pub/Sub subscriptions.
# This metric is designed to be used for 'page views per minute' calculations in dashboards and alerts.
resource "google_logging_metric" "page_views_metric" {
  name        = "page-views-count-pubsub"
  description = "Counts 'page view' events extracted from Pub/Sub subscription logs, used for per-minute rates."
  project     = var.project

  # Filter for log entries that represent a page view processed via Pub/Sub.
  # This filter targets logs from Pub/Sub subscriptions (resource.type="pubsub_subscription")
  # and specifically looks for 'jsonPayload.eventType="page_view"'.
  # Adjust 'jsonPayload.eventType="page_view"' if your logs use a different field
  # or format to indicate a page view from a Pub/Sub message.
  filter = "resource.type=\"pubsub_subscription\" AND jsonPayload.eventType=subscription"

  # Metric descriptor defines the type and properties of the metric.
  # metric_kind = DELTA means it's a counter that accumulates values over time.
  # value_type  = INT64 means the count is an integer.
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

# --- Resource 2: Google Cloud Monitoring Notification Channel (Email) ---
# This resource sets up an email notification channel for alerts.
# You will receive an email at the specified address when an alert triggers.
# IMPORTANT: After Terraform applies this, Google Cloud will send a verification
# email to the specified address. You MUST click the link in that email to
# enable this notification channel.
resource "google_monitoring_notification_channel" "email_channel" {
  display_name = "Alerts Email"
  type         = "email"
  labels = {
    "email_address" = var.alert_email_address
  }
  # Enable the channel immediately. Verification is still required.
  enabled = true
}

# --- Resource 3: Google Cloud Monitoring Alert Policy ---
# This resource creates an alerting policy that monitors the 'page_views_count_pubsub' metric.
# It triggers an alert if the rate of page views per minute exceeds a certain threshold.
resource "google_monitoring_alert_policy" "page_views_alert" {
  display_name = "High Pub/Sub Page View Rate Alert (Per Minute)"
  combiner     = "OR" # Trigger if any condition is met

  # Condition for the alert: Page views rate threshold
  conditions {
    display_name = "Page views per minute exceeds 5 via Pub/Sub"
    condition_threshold {
      # The metric being monitored: the custom log-based metric for Pub/Sub
      filter = "metric.type=\"logging.googleapis.com/user/page_views_count_pubsub\""

      # Define how to aggregate the metric over time to get a 'per minute' rate.
      # ALIGN_RATE converts the delta metric to a rate (per second).
      # REDUCE_SUM sums up all values.
      # The period is 60 seconds (1 minute).
      aggregations {
        alignment_period   = "60s"       # Align data points to 1-minute intervals
        per_series_aligner = "ALIGN_RATE" # Convert counter to rate (per second)
        cross_series_reducer = "REDUCE_SUM" # Sum across all series
      }

      # Duration for which the threshold must be true before alerting
      duration        = "300s" # 5 minutes

      # The comparison operator (e.g., greater than)
      comparison      = "COMPARISON_GT"

      # The threshold value.
      # Since ALIGN_RATE gives units per second, and we want "per minute",
      # a threshold of 100 (per minute) means 100/60 = ~1.667 per second.
      threshold_value = 1.667 # Equivalent to 100 page views per minute (100/60)

      # Evaluation missing data: don't alert if data is missing
      evaluations_missing_data = "EVALUATION_MISSING_DATA_NO_OP"
    }
  }

  # Link the notification channel to the alert policy
  notification_channels = [
    google_monitoring_notification_channel.email_channel.id,
  ]

  # Documentation for the alert
  documentation {
    content = "This alert triggers when the rate of successful 'page view' events processed via a Pub/Sub subscription exceeds 100 per minute."
    mime_type = "text/markdown"
  }

  # Severity of the alert (e.g., CRITICAL, WARNING, INFO)
  severity = "WARNING"
}

# --- Resource 4: Google Cloud Monitoring Dashboard ---
# This resource creates a custom dashboard to visualize the 'page_views_count_pubsub' metric as a 'per minute' rate.
resource "google_monitoring_dashboard" "page_views_dashboard" {
  display_name = "Pub/Sub Page Views Dashboard (Per Minute)"
  project      = var.project

  # Dashboard layout and widgets
  dashboard_json = jsonencode({
    "displayName" = "Pub/Sub Page Views Dashboard (Per Minute)"
    "gridLayout" = {
      "columns" = "2"
      "widgets" = [
        {
          "title" = "Page Views (Per Minute) from Pub/Sub"
          "xyChart" = {
            "dataSets" = [
              {
                "timeSeriesQuery" = {
                  # Filter for the custom log-based metric from Pub/Sub subscriptions
                  "filter" = "metric.type=\"logging.googleapis.com/user/page_views_count_pubsub\" resource.type=\"pubsub_subscription\""
                  "aggregation" = {
                    "alignmentPeriod" = "60s"          # Align data points to 1-minute intervals
                    "perSeriesAligner" = "ALIGN_RATE"    # Convert counter to rate (per second)
                    "crossSeriesReducer" = "REDUCE_SUM"  # Sum across all series
                  }
                  "unitOverride" = "count" # Display as a count
                }
                "plotType" = "LINE"
              }
            ]
            "yAxis" = {
              "label" = "Page Views (Per Minute)"
              "scale" = "LINEAR"
            }
          }
        }
      ]
    }
  })
}

# --- Outputs ---
output "log_metric_name" {
  description = "The name of the created log-based metric for Pub/Sub page views, designed for per-minute rates."
  value       = google_logging_metric.page_views_metric.name
}

output "alert_policy_name" {
  description = "The name of the created monitoring alert policy for Pub/Sub page views (per minute)."
  value       = google_monitoring_alert_policy.page_views_alert.display_name
}

output "dashboard_name" {
  description = "The name of the created monitoring dashboard for Pub/Sub page views (per minute)."
  value       = google_monitoring_dashboard.page_views_dashboard.display_name
}
