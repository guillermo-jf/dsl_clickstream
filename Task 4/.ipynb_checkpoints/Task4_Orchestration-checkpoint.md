## Task 4: Using Google Cloud Composer to orchestrate data engineering tasks

In this task, you use Apache Airflow and Google Cloud Composer to automate a data engineering task. 

1. Create a Composer pipeline with the following requirements:
   1. When a file containing clickstream data is written to a Cloud Storage bucket, trigger the pipeline. 
   2. Run a Dataflow job that processes the file, parses the data, and writes it to BigQuery. 
   3. After the Dataflow job completes, send a message to Pub/Sub indicating the file was processed. 

2. Create a subscriber to the Pub/Sub topic that notifies you that the file was successfully processed. 