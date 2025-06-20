echo "Starting lab setup..."

# Enable the requires APIs for the Dataplex class
gcloud services enable compute.googleapis.com \
bigquery.googleapis.com \
bigqueryconnection.googleapis.com \
dataplex.googleapis.com \
datalineage.googleapis.com \
dataflow.googleapis.com \
dataproc.googleapis.com \
datacatalog.googleapis.com \
metastore.googleapis.com \
datapipelines.googleapis.com \
cloudscheduler.googleapis.com \
dlp.googleapis.com 



# Create the BQ Dataset and table
bq mk dsl_project_datamesh
bq load --autodetect dsl_project_datamesh.visits gs://jellyfish-training-demo-6/dsl-project/visits-2024-07-01.jsonl

###################################
### Web visits Example
###################################

# Create the Lake for Web visits
echo "Creating clickstream service data lake..."
gcloud dataplex lakes create dsl-lake-clickstream \
 --project=$GOOGLE_CLOUD_PROJECT \
 --location=us-central1 \
 --labels=team=clickstream-analytics \
 --display-name="DSL Challenge Lab.Clickstream Data"

# Create the Zone for Web Request Data
echo "Creating asset..."
gcloud dataplex zones create clickstream-events-zone \
 --project=$GOOGLE_CLOUD_PROJECT \
 --lake=dsl-lake-clickstream \
 --resource-location-type=MULTI_REGION \
 --location=us-central1 \
 --type=RAW \
 --discovery-enabled \
 --discovery-schedule="0 * * * *" \
 --labels=data_product_category=raw_data

# Create the asset for the BQ Data
gcloud dataplex assets create clickstream-events-asset \
 --project=$GOOGLE_CLOUD_PROJECT \
 --location=us-central1 \
 --lake=dsl-lake-clickstream \
 --zone=clickstream-events-zone \
 --resource-type=BIGQUERY_DATASET \
 --resource-name=projects/$GOOGLE_CLOUD_PROJECT/datasets/dsl_project_datamesh
