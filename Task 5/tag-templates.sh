# Define your project ID and a region
PROJECT_ID="jellyfish-training-demo-6" # Your project ID
REGION="us-central1"                  # Choose an appropriate region

# Define your tag template ID and display name
TAG_TEMPLATE_ID="data_classification_template"
DISPLAY_NAME="Data Classification"
DESCRIPTION="A template for classifying data sensitivity and PII presence."

# 1. Create the base tag template
# The fields are defined using the --field flag, where each field is a comma-separated
# list of key-value pairs (id=FIELD_ID,display-name=DISPLAY_NAME,type=TYPE)
# For enumerated types, you also specify allowed-values.

gcloud data-catalog tag-templates create "${TAG_TEMPLATE_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --display-name="${DISPLAY_NAME}" \
    --description="${DESCRIPTION}" \
    --field=id="sensitivity",display-name="Sensitivity Level",type="ENUM",allowed-values="PUBLIC,CONFIDENTIAL,RESTRICTED" \
    --field=id="has_pii",display-name="Contains PII",type="BOOL" \
    --field=id="owner_email",display-name="Data Owner Email",type="STRING"

echo "Tag template '${TAG_TEMPLATE_ID}' created successfully."

# 2. (Optional) Describe the created tag template to verify
echo "Describing the created tag template:"
gcloud data-catalog tag-templates describe "${TAG_TEMPLATE_ID}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}"
