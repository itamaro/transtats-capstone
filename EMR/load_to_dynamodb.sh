#!/bin/bash

source emr_util.sh

usage_and_die() {
  echo "Usage: $0 [cluster_id] [dataset]" >&2
  exit 42
}

if [ "$#" != "2" ]; then
  usage_and_die
fi

CLUSTER_ID="$1"
DATASET="$2"
STEPS_DIR="$( cd $( dirname $0 )/generic-steps && pwd )"
LOCAL_HIVE_DIR="$( cd $( dirname $0 )/.. && pwd )/Hive"
REMOTE_HIVE_DIR="s3://itamaro/hive"

if [[ ! $CLUSTER_ID =~ j-* ]]; then
  usage_and_die
fi

if [ ! -d "$STEPS_DIR" ]; then
  echo "Could not find steps directory at $STEPS_DIR" >&2
  exit 42
fi

if [ ! -d "$LOCAL_HIVE_DIR" ]; then
  echo "Could not find local hive scripts directory at $LOCAL_HIVE_DIR" >&2
  exit 42
fi

echo "Consider running \"./update_dynamodb_tables_capacity.sh\" now" \
     "to pump up write throughput for the duration of insertions to DynamoDB"
echo "Recommended for small dataset:"
echo "./update_dynamodb_tables_capacity.sh $DATASET 1000 15000"
echo "Recommended for large dataset:"
echo "./update_dynamodb_tables_capacity.sh $DATASET 1000 50000"

HIVE_TMPL="$LOCAL_HIVE_DIR/load_data.q.j2"
echo "Rendering Hive script from $HIVE_TMPL and uploading to S3 at $REMOTE_HIVE_DIR"
python render_step.py "$HIVE_TMPL" -v dynamodb_prefix="$DATASET" |
    aws s3 cp - "$REMOTE_HIVE_DIR/load_data.q"

echo "Submitting Hive-formatter jobs"
aws emr add-steps --cluster-id $CLUSTER_ID \
    --steps file://$STEPS_DIR/hive_formatter.json

echo "Submitting Load-to-DynamoDB-with-Hive job"
aws emr add-steps --cluster-id $CLUSTER_ID \
    --steps Type=HIVE,Name='Load Results to DynamoDB Using Hive',ActionOnFailure=CANCEL_AND_WAIT,Args=[-f,"$REMOTE_HIVE_DIR/load_data.q"]

wait_for_ready_cluster $CLUSTER_ID

echo "Finished insetions - scaling down write capacity to save costs"
./update_dynamodb_tables_capacity.sh "$DATASET" 10 10
