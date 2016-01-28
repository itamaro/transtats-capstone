#!/bin/bash

PREFIX="$1"
WRITE_CAPACITY=5000

if [ -z "$PREFIX" ]; then
  echo "Usage: $0 [tables_prefix]" >&2
  exit 42
fi
echo "Creating DynamoDB Tables for questions in Group2 & Tom's Challenge using prefix $PREFIX"

aws dynamodb create-table --table-name "${PREFIX}.g2q1" \
    --attribute-definitions AttributeName=col_id,AttributeType=S \
                            AttributeName=origin,AttributeType=S \
    --key-schema AttributeName=col_id,KeyType=HASH \
                 AttributeName=origin,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=$WRITE_CAPACITY | \
    echo "Creating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb create-table --table-name "${PREFIX}.g2q2" \
    --attribute-definitions AttributeName=col_id,AttributeType=S \
                            AttributeName=origin,AttributeType=S \
    --key-schema AttributeName=col_id,KeyType=HASH \
                 AttributeName=origin,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=$WRITE_CAPACITY | \
    echo "Creating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb create-table --table-name "${PREFIX}.g2q4" \
    --attribute-definitions AttributeName=col_id,AttributeType=S \
                            AttributeName=origin,AttributeType=S \
    --key-schema AttributeName=col_id,KeyType=HASH \
                 AttributeName=origin,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=$WRITE_CAPACITY | \
    echo "Creating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb create-table --table-name "${PREFIX}.g3q2" \
    --attribute-definitions AttributeName=col_id,AttributeType=S \
    --key-schema AttributeName=col_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=$WRITE_CAPACITY | \
    echo "Creating table $( python get_json_field.py TableDescription TableArn )"
