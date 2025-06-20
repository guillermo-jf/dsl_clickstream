#!/bin/zsh
gcloud functions deploy process_visit_message \
    --runtime python313 \ 
    --trigger-topic dsl-project-clickstream \
    --entry-point process_pubsub_message \
    --region us-central1 \
    --set-env-vars GCP_PROJECT=$GOOGLE_ClOUD_PROJECT,BIGQUERY_DATASET=dsl-project,BIGQUERY_TABLE=web_visits \
    --source .