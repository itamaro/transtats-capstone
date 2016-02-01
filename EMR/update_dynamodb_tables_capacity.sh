#!/bin/bash

PREFIX="$1"
W_CAP="$2"
TW_CAP="$3"
DYNAMODB_GSI_DIR="$( cd $( dirname $0 )/dynamo-gsi && pwd )"

if [ -z "$PREFIX" ]; then
  echo "Usage: $0 [tables_prefix] [write_capacity(=1000)] [tom_write_capacity(=25000)]" >&2
  exit 42
fi

if [ -z "$W_CAP" ]; then
  W_CAP="1000"
fi

if [ -z "$TW_CAP" ]; then
  TW_CAP="25000"
fi

if [ ! -d "$DYNAMODB_GSI_DIR" ]; then
  echo "Could not find DynamoDB GSI JSON's directory at $DYNAMODB_GSI_DIR" >&2
  exit 42
fi

echo "Updating write-capacity for all tables with prefix $PREFIX ($W_CAP, $TW_CAP)"

UPDATE_GSI="$( mktemp )"
python render_step.py "$DYNAMODB_GSI_DIR/update-origin-delay-gsi.json" \
    -v write_capacity="$W_CAP" > "$UPDATE_GSI"

aws dynamodb update-table --table-name "${PREFIX}.g2q1" \
    --global-secondary-index-updates file://$UPDATE_GSI \
    --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits="$W_CAP" | \
    echo "Updating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb update-table --table-name "${PREFIX}.g2q2" \
    --global-secondary-index-updates file://$UPDATE_GSI \
    --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits="$W_CAP" | \
    echo "Updating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb update-table --table-name "${PREFIX}.g2q4" \
    --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits="$W_CAP" | \
    echo "Updating table $( python get_json_field.py TableDescription TableArn )"

aws dynamodb update-table --table-name "${PREFIX}.g3q2" \
    --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits="$TW_CAP" | \
    echo "Updating table $( python get_json_field.py TableDescription TableArn )"

rm "$UPDATE_GSI"
