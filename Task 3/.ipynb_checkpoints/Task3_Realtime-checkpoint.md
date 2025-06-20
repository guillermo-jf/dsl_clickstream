## Task 3: Processing the data in real time

In this task, you use Google tools to process data in real time. The data is sent to a Pub/Sub topic. You program subscribers to process the messages as they arrive. 

1. Write a simulator that creates visits and posts them as messages to a Google Pub/Sub topic. Use variables to control the number of visits sent per minute and how long the simulator should run. You can program this any way you like. 

* See: **gen_events_simulator.py**


2. Create a push subscriber running in Cloud Run or Cloud Functions. Process the messages as they come in. Parse them, and write the data to BigQuery where it can be analyzed. In Looker Studio, create a simple report that shows clicks by page in real time. 

* **Cloud Run Function Deployment. See Push folder**

3. Do the same thing as the previous step, but program a pull subscriber. Deploy the program to a Compute Engine virtual machine. Use an instance group to set up autoscaling. Also, implement some kind of health check that you can use to ensure if the pull process is not running, the machine will be restarted. 

* Check Terraform script to create **instance template and MIG:** util/mig_setup_realtime.tf

4. Write an Apache Beam pipeline with the following requirements:
   1. Write the raw data to files in Google Cloud Storage at regular intervals. 
   2. Parse the messages and write the data to BigQuery. 
   3. Calculate page views by minute. Create a dashboard that reports this information.
   4. Run the pipeline in Dataflow. 

* Check Terraform script to create **Log based metric for Monitoring/Alerting:** util/page_views_monitoring.tf

5. You want to detect a potential denial of service attack. Create an Apache Beam pipeline that calculates page views per minute. Write this information to the Google Cloud logs. Create a log metric that reports this information in a Logging and Monitoring Dashboard. Next, create a log alert that triggers beyond some threshold. When the alert triggers, send yourself an email. 

* See: **denial_of_service_pipeline.py**


6. Restart your Pub/Sub message simulator so enough messages are sent to trigger the alert. 


## Running the Visit Simulator (`Task3/simulator.py`)

This script simulates user visits and publishes them as messages to a Google Cloud Pub/Sub topic. It generates visit data based on the schema defined in `dsllib/table_schema.json` and uses product information from `Task3/products.json`.

### Prerequisites

1.  **Google Cloud Project**: You need a Google Cloud Project with the Pub/Sub API enabled.
2.  **Pub/Sub Topic**: Create a Pub/Sub topic to receive the simulated visit messages.
3.  **Authentication**: Ensure you have authenticated with Google Cloud, typically by running `gcloud auth application-default login` or by setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
4.  **Install Dependencies**: Install the necessary Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Simulator

Navigate to the root directory of the repository and run the script using the following command structure:

```bash
python Task3/simulator.py --project_id YOUR_PROJECT_ID --topic_id YOUR_TOPIC_ID [OPTIONS]
```

#### Command-Line Arguments:

*   `--project_id` (Required): Your Google Cloud Project ID.
*   `--topic_id` (Required): The ID of your Pub/Sub topic.
*   `--visits_per_minute` (Optional): The number of visit messages to generate and send per minute. Defaults to `60`.
*   `--duration_minutes` (Optional): The total time in minutes the simulator should run. Defaults to `1`.

#### Example:

To run the simulator for 5 minutes, sending messages to the topic `my-visits-topic` in project `my-gcp-project` at a rate of 120 visits per minute:

```bash
python Task3/simulator.py --project_id my-gcp-project --topic_id my-visits-topic --visits_per_minute 120 --duration_minutes 5
```

## Step 5: Denial of Service Detection Pipeline

This step focuses on using the Apache Beam pipeline (`Task3/denial_of_service_pipeline.py`) to calculate page views per minute, write this data to Google Cloud Logging, and then set up monitoring and alerting based on these logs.

### Running the Pipeline

The `denial_of_service_pipeline.py` script processes messages from a Pub/Sub topic, calculates page views per minute, and logs this information as structured JSON to Google Cloud Logging.

**Command to Run:**

```bash
python Task3/denial_of_service_pipeline.py \
    --project_id YOUR_PROJECT_ID \
    --topic_id YOUR_PUBSUB_TOPIC_ID \
    --runner DataflowRunner \
    --region YOUR_DATAFLOW_REGION \
    --temp_location gs://YOUR_BUCKET_NAME/temp/ \
    --job_name dos-detector-`date +%Y%m%d-%H%M%S`
```

**Command-Line Arguments:**

*   `--project_id`: (Required) Your Google Cloud Project ID.
*   `--topic_id`: (Required) The ID of the Pub/Sub topic from which the pipeline will read messages.
*   `--runner`: (Optional) The Beam runner to use. For production, `DataflowRunner` is recommended. For local testing, `DirectRunner` can be used. Defaults to `DirectRunner` if not specified.
*   `--region`: (Optional) The Google Cloud region where the Dataflow job will run (e.g., `us-central1`). Required if using `DataflowRunner`.
*   `--temp_location`: (Required) A GCS path for Dataflow to store temporary files (e.g., `gs://your-gcs-bucket-name/temp/`).
*   `--job_name`: (Optional) A unique name for the Dataflow job. The example uses `date` to append a timestamp, ensuring uniqueness.

**Python Path and `dsllib`:**

The pipeline script attempts to import `parse_visit` from `dsllib.visits`. It includes logic to add the parent directory of `Task3` to `sys.path` if the initial import fails. This often works for local execution if `dsllib` is in the expected location (i.e., the root of the repository, alongside the `Task3` directory).

However, when running with `DataflowRunner`:
*   Ensure `dsllib` is available to the Dataflow workers. This might involve:
    *   Packaging `dsllib` with your pipeline using a `setup.py` file. You would typically list `dsllib` (or the directory containing it if it's not a standard package) in the `setup.py` and run the pipeline script as a module.
    *   Specifying `dsllib` as an extra package if it's structured as a Python package and uploaded to a location accessible by the workers.
*   If you encounter import errors related to `dsllib` on Dataflow, you will need to configure the pipeline's packaging options to include `dsllib`.

### Creating a Log-Based Metric

Log-based metrics allow you to create metrics from the content of your log entries. We'll create one to count the number of "page_view_stats" log entries generated by our pipeline.

1.  **Navigate to Log-based Metrics**:
    *   In the Google Cloud Console, go to "Logging" > "Log-based Metrics".
    *   Click "CREATE METRIC".

2.  **Select Metric Type**:
    *   Choose "Counter" as the metric type. This type is suitable for counting the number of log entries that match your filter.

3.  **Define the Metric**:
    *   **Log filter**: This is crucial for selecting the correct log entries. Use the following filter:
        ```
        resource.type="dataflow_step"
        jsonPayload.log_type="page_view_stats"
        ```
        *   `resource.type="dataflow_step"`: This targets logs generated by Dataflow jobs. If your pipeline runs with a different runner or context, you might need to adjust this (e.g., `k8s_container` if running on GKE, or simply omit if logs are not specific to a resource type but have unique payload fields).
        *   `jsonPayload.log_type="page_view_stats"`: This selects the specific JSON log entries our pipeline generates.
        *   You can verify this filter in the Logs Explorer to ensure it matches the logs from your pipeline.

    *   **Metric Details**:
        *   **Metric name**: `page_views_per_minute_log_count` (or a name of your choice, e.g., `dos_detection_page_view_windows`).
        *   **Description**: "Counts the number of 1-minute window summaries of page views generated by the denial_of_service_pipeline. Each count represents one minute of processed data."
        *   **Units**: `1` (since we are counting entries).
        *   **Labels**: For this specific metric, custom labels are not strictly necessary for the alerting goal. The value of `page_views_per_minute` is *inside* the log entry and will be used when defining the alert condition, not as a metric label itself.

4.  **Click "Create Metric"**. It might take a minute or two for the metric to become available and start collecting data after matching log entries are found.

### Creating a Log-Based Alert

Now, create an alert that triggers when the rate of page views (as indicated by our log entries and their content) exceeds a certain threshold.

1.  **Navigate to Monitoring > Alerting**:
    *   In the Google Cloud Console, go to "Monitoring" > "Alerting".
    *   Click "+ CREATE POLICY".

2.  **Select Metric**:
    *   In the "Select a metric" search box, type the name of the log-based metric you created (e.g., `page_views_per_minute_log_count`). It should appear under "Logging > User-defined metrics". Select it.
    *   Ensure the "Resource type" is appropriate (e.g., "Dataflow Job" or "Global").
    *   Click "Apply".

3.  **Configure Alert Trigger**:
    *   This is where you define what constitutes an alert. We want to alert when the *value* reported *inside* our log entries (i.e., `jsonPayload.page_views_per_minute`) is high.
    *   Since our *metric* counts log entries (one per minute), we need to configure the alert to look *into* the log entries associated with that metric. This is often done by ensuring the log-based metric extracts the relevant value or by using features of the alerting system that can parse fields from the logs associated with the metric count.
    *   **Important Clarification**: Standard log-based counter metrics count *entries*, not values *within* them. To alert on `jsonPayload.page_views_per_minute`, you have two main options:
        1.  **Use a Distribution Log-Based Metric**: When creating the log-based metric, choose "Distribution" instead of "Counter". Then, under "Fields" for the distribution, specify `jsonPayload.page_views_per_minute` as the field to extract values from. This allows you to alert on percentiles, averages, or counts of these extracted values. This is the **recommended approach for alerting on the actual value**.
        2.  **Alert on Log Volume (Simpler, but less direct)**: If you stick with a "Counter" metric (like `page_views_per_minute_log_count`), it just counts how many "page_view_stats" logs arrive. This isn't directly alerting on the *number* of page views. You'd be alerting that "a summary log was received", not "the summary log reported too many views".

    Let's assume you will **recreate the log-based metric as a Distribution metric** named `page_views_per_minute_distribution` extracting `jsonPayload.page_views_per_minute`.

    *   **If using a Distribution Metric (`page_views_per_minute_distribution` extracting `jsonPayload.page_views_per_minute`):**
        *   **Condition triggers if**: "Any time series violates"
        *   **Condition**:
            *   Aggregator: `mean` (or `percentile 99`, `max`, etc., depending on how you want to measure) - choose an aggregator that makes sense for your data.
            *   "is above"
            *   **Threshold**: `1000` (Example: alert if the average page views reported in a log entry within the alignment period is over 1000. Adjust this value based on your expected traffic and what you consider a DoS attempt.)
            *   **For**: "1 minute" (or a suitable duration, e.g., "5 minutes" to trigger if the condition persists). This refers to how long the threshold must be breached.
        *   **Alignment Period (Advanced options)**: This should ideally align with how often your pipeline logs (i.e., 1 minute).
        *   **Filter (Optional but recommended)**: You can add filters here to be very specific, for example, if you have multiple Dataflow jobs logging similar metrics. `metric.label.job_name = "your-dataflow-job-name"` could be useful.

4.  **Notifications**:
    *   Under "Notifications and name", click "Notification Channels".
    *   Select an existing email notification channel or click "Manage Notification Channels" to add a new one.
    *   To add a new email channel:
        *   Scroll down to "Email" and click "ADD NEW".
        *   Enter an "Email address" and a "Display name".
        *   Click "Save".
    *   Select your desired email channel(s) for the alert.

5.  **Name and Save the Alert Policy**:
    *   Give your alert policy a descriptive name (e.g., "High Page View Rate - Possible DoS").
    *   Review the documentation section if desired.
    *   Click "SAVE POLICY".

### Configuring Email Notifications

This was covered in the "Creating a Log-Based Alert" section (Step 4 under Notifications). When you create or edit an alert policy, you can select or create notification channels. Email is a common type.

1.  During alert creation/editing, find the "Notifications" section.
2.  Click "SELECT NOTIFICATION CHANNELS" (or similar wording).
3.  If you have an existing email channel, select it.
4.  If not, click "MANAGE NOTIFICATION CHANNELS". This takes you to the Notification channels page in Monitoring.
    *   Click "ADD NEW" for the Email type.
    *   Provide your email address and a display name for the channel.
    *   Save the channel.
5.  Go back to your alert policy configuration and select the newly created (or existing) email channel.

Once the alert is configured and the pipeline is running, if the conditions are met (e.g., page views per minute reported in the logs exceed your threshold), the alert will trigger and send an email to the configured address.