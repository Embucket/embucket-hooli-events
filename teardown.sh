#!/usr/bin/env bash
set -euo pipefail

REGION="us-east-2"
STACK_PREFIX="hooli-events"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

echo "=== Tearing down Hooli Events Pipeline ==="
echo "This will delete ALL resources. Press Ctrl-C to abort."
read -r -p "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || exit 0

# Delete in reverse order
echo "--- Deleting compute stack ---"
aws cloudformation delete-stack --stack-name "${STACK_PREFIX}-compute" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_PREFIX}-compute" --region "$REGION"

# Delete ECR images before deleting repos
echo "--- Cleaning ECR repositories ---"
for repo in hooli-events/collector hooli-events/enrich hooli-events/lake-loader; do
  aws ecr batch-delete-image \
    --repository-name "$repo" \
    --image-ids "$(aws ecr list-images --repository-name "$repo" --region "$REGION" --query 'imageIds' --output json)" \
    --region "$REGION" 2>/dev/null || true
done

echo "--- Deleting data stack ---"
aws cloudformation delete-stack --stack-name "${STACK_PREFIX}-data" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_PREFIX}-data" --region "$REGION"

echo "--- Deleting network stack ---"
aws cloudformation delete-stack --stack-name "${STACK_PREFIX}-network" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_PREFIX}-network" --region "$REGION"

# Delete Glue catalog
echo "--- Deleting Glue catalog ---"
aws glue delete-catalog --name hooli-s3tables --region "$REGION" 2>/dev/null || true

# Delete KCL DynamoDB tables
echo "--- Deleting KCL DynamoDB tables ---"
aws dynamodb delete-table --table-name hooli-enrich --region "$REGION" 2>/dev/null || true
aws dynamodb delete-table --table-name hooli-lake-loader --region "$REGION" 2>/dev/null || true

echo "=== Teardown Complete ==="
