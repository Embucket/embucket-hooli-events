#!/usr/bin/env bash
set -euo pipefail

REGION="us-east-2"
STACK_PREFIX="hooli-events"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

echo "=== Deploying Hooli Events Pipeline ==="
echo "Account: $ACCOUNT_ID  Region: $REGION"
echo ""

# 1. Network stack
echo "--- Deploying network stack ---"
aws cloudformation deploy \
  --template-file cloudformation/network.yaml \
  --stack-name "${STACK_PREFIX}-network" \
  --region "$REGION" \
  --no-fail-on-empty-changeset

# 2. Data stack
echo "--- Deploying data stack ---"
aws cloudformation deploy \
  --template-file cloudformation/data.yaml \
  --stack-name "${STACK_PREFIX}-data" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset

# 3. Get ECR repo URIs and table bucket ARN
get_output() {
  aws cloudformation describe-stacks \
    --stack-name "$1" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}

COLLECTOR_REPO=$(get_output "${STACK_PREFIX}-data" "CollectorRepoUri")
ENRICH_REPO=$(get_output "${STACK_PREFIX}-data" "EnrichRepoUri")
LAKE_LOADER_REPO=$(get_output "${STACK_PREFIX}-data" "LakeLoaderRepoUri")
EVENT_GENERATOR_REPO=$(get_output "${STACK_PREFIX}-data" "EventGeneratorRepoUri")
TABLE_BUCKET_ARN=$(get_output "${STACK_PREFIX}-data" "TableBucketArn")

echo "Collector repo:       $COLLECTOR_REPO"
echo "Enrich repo:          $ENRICH_REPO"
echo "Lake Loader repo:     $LAKE_LOADER_REPO"
echo "Event Generator repo: $EVENT_GENERATOR_REPO"
echo "Table bucket ARN:     $TABLE_BUCKET_ARN"

# 4. Build and push Docker images
echo "--- Building and pushing Docker images ---"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

docker build --platform linux/amd64 -f docker/collector/Dockerfile -t "${COLLECTOR_REPO}:latest" config/
docker push "${COLLECTOR_REPO}:latest"

docker build --platform linux/amd64 -f docker/enrich/Dockerfile -t "${ENRICH_REPO}:latest" config/
docker push "${ENRICH_REPO}:latest"

docker build --platform linux/amd64 -f docker/lake-loader/Dockerfile -t "${LAKE_LOADER_REPO}:latest" config/
docker push "${LAKE_LOADER_REPO}:latest"

docker build --platform linux/amd64 -f docker/event-generator/Dockerfile -t "${EVENT_GENERATOR_REPO}:latest" config/
docker push "${EVENT_GENERATOR_REPO}:latest"

# 5. Deploy compute stack
echo "--- Deploying compute stack ---"
aws cloudformation deploy \
  --template-file cloudformation/compute.yaml \
  --stack-name "${STACK_PREFIX}-compute" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    "CollectorImageUri=${COLLECTOR_REPO}:latest" \
    "EnrichImageUri=${ENRICH_REPO}:latest" \
    "LakeLoaderImageUri=${LAKE_LOADER_REPO}:latest" \
    "EventGeneratorImageUri=${EVENT_GENERATOR_REPO}:latest" \
    "TableBucketArn=${TABLE_BUCKET_ARN}" \
    "CollectorDesiredCount=${COLLECTOR_DESIRED_COUNT:-0}" \
    "EnrichDesiredCount=${ENRICH_DESIRED_COUNT:-0}"

# 6. Get collector endpoint
ALB_DNS=$(get_output "${STACK_PREFIX}-compute" "CollectorEndpoint")

# 7. Create Glue catalog integration for Athena
echo "--- Creating Glue catalog for Athena ---"
aws glue create-catalog \
  --name hooli-s3tables \
  --catalog-input "{\"FederatedCatalog\":{\"Identifier\":\"${TABLE_BUCKET_ARN}\",\"ConnectionName\":\"aws:s3tables\"}}" \
  --region "$REGION" 2>/dev/null \
  && echo "Glue catalog created" \
  || echo "Glue catalog already exists (or creation skipped)"

echo ""
echo "=== Deployment Complete ==="
echo "Collector endpoint: http://${ALB_DNS}"
echo ""
echo "Next steps:"
echo "  1. Update website/js/tracker.js: set COLLECTOR_ENDPOINT = \"http://${ALB_DNS}\""
echo "  2. Serve the website:  cd website && python3 -m http.server 8000"
echo "  3. Run the simulator:  python3 simulator/simulate.py --endpoint http://${ALB_DNS}"
echo "  4. Query in Athena:    SELECT * FROM \"hooli-s3tables\".\"analytics\".\"enriched_events\" LIMIT 10"
