#!/bin/bash

PREFIX="$1"

if [ -z "$PREFIX" ]; then
  echo "Usage: $0 [tables_prefix]" >&2
  exit 42
fi

echo "Deleting DynamoDB Tables for questions in Group2 & Tom's Challenge using prefix $PREFIX"

aws dynamodb delete-table --table-name "${PREFIX}.g2q1"
aws dynamodb delete-table --table-name "${PREFIX}.g2q2"
aws dynamodb delete-table --table-name "${PREFIX}.g2q4"
aws dynamodb delete-table --table-name "${PREFIX}.g3q2"
