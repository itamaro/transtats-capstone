#!/bin/bash

PREFIX="$1"
DYNAMODB_GSI_DIR="$( cd $( dirname $0 )/dynamo-gsi && pwd )"
ADD_GSI_JSON="$DYNAMODB_GSI_DIR/add-origin-delay-gsi.json"

if [ -z "$PREFIX" ]; then
  echo "Usage: $0 [tables_prefix]" >&2
  exit 42
fi

if [ ! -r "$ADD_GSI_JSON" ]; then
  echo "Could not read DynamoDB GSI JSON file $ADD_GSI_JSON" >&2
  exit 42
fi

echo "Adding indices to g2q{1,2} tables with prefix $PREFIX"

aws dynamodb wait table-exists --table-name "${PREFIX}.g2q1"
aws dynamodb update-table --table-name "${PREFIX}.g2q1" \
    --attribute-definitions AttributeName=origin,AttributeType=S \
                            AttributeName=delay,AttributeType=N \
    --global-secondary-index-updates file://$ADD_GSI_JSON | \
    echo "Adding GSI to table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb wait table-exists --table-name "${PREFIX}.g2q2"
aws dynamodb update-table --table-name "${PREFIX}.g2q2" \
    --attribute-definitions AttributeName=origin,AttributeType=S \
                            AttributeName=delay,AttributeType=N \
    --global-secondary-index-updates file://$ADD_GSI_JSON | \
    echo "Adding GSI to table $( python get_json_field.py TableDescription TableArn )"
