#!/bin/bash

usage_and_die() {
  echo "Usage: $0 [dataset] [stack] [emr_core_nodes(=1)]" >&2
  exit 42
}

if [ $# -lt 2 ]; then
  usage_and_die
fi

DATASET="$1"
STACK="$2"
NODES="$3"
TMPL_DIR="$( cd $( dirname $0 )/step-templates && pwd )"
REMOTE_CODE_DIR="s3://itamaro/code"

if [[ "$STACK" == "hadoop" ]]; then
  CODE_DIR="MapReduce"
elif [[ "$STACK" == "spark" ]]; then
  CODE_DIR="Spark"
else
  echo "Only Hadoop and Spark supported at the moment" >&2
  exit 42
fi

LOCAL_CODE_DIR="$( cd $( dirname $0 )/.. && pwd )/$CODE_DIR"

if [ ! -d "$TMPL_DIR" ]; then
  echo "Could not find steps templates directory at $TMPL_DIR" >&2
  exit 42
fi

if [ ! -d "$LOCAL_CODE_DIR" ]; then
  echo "Could not find local code directory at $LOCAL_CODE_DIR" >&2
  exit 42
fi

if [ -z "$NODES" ]; then
  NODES="1"
fi

echo "Starting run on dataset s3://itamaro/$DATASET at $(date)"

echo "Copying latest code from $LOCAL_CODE_DIR to S3 at $REMOTE_CODE_DIR"
aws s3 cp "$LOCAL_CODE_DIR" "$REMOTE_CODE_DIR" --recursive

echo "Creating EMR Cluster with $NODES core nodes (1 master)"
IMPORT_STEP="$( mktemp )"
python render_step.py "$TMPL_DIR/import_dataset.json.j2" \
    -v input_data_dir="$DATASET" > "$IMPORT_STEP"
CLUSTER_ID=$( aws emr create-cluster \
    --name my-emr-cluster \
    --release-label emr-4.2.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=$NODES,InstanceType=m3.xlarge \
    --use-default-roles \
    --no-termination-protected \
    --no-auto-terminate \
    --ec2-attributes AvailabilityZone=us-east-1a \
    --steps file://$IMPORT_STEP \
    --bootstrap-actions '[{"Path":"s3://itamaro/emr.bootstrap/install_kafka.rb","Name":"Install Kafka"}]' \
    --applications Name=Hadoop Name=Spark Name=Hive | \
    python get_json_field.py ClusterId )
echo "Created EMR Cluster ID $CLUSTER_ID"
rm "$IMPORT_STEP"

# ./create_dynamodb_tables.sh "$DATASET"

echo "Waiting for cluster $CLUSTER_ID to be provisioned and bootstrapped..."
aws emr wait cluster-running --cluster-id $CLUSTER_ID
echo "Cluster $CLUSTER_ID is ready for next steps!"

./solve_questions.sh "$CLUSTER_ID" "$STACK"

echo "Backing up HDFS outputs in S3"
BACKUP_STEP="$( mktemp )"
python render_step.py "$TMPL_DIR/backup_output.json.j2" \
    -v backup_data_dir="$DATASET" > "$BACKUP_STEP"
aws emr add-steps --cluster-id $CLUSTER_ID --steps file://$BACKUP_STEP
rm "$BACKUP_STEP"

# ./load_to_dynamodb.sh "$CLUSTER_ID" "${DATASET}.${STACK}"

echo "~~~ Summary of all steps: ~~~"
aws emr list-steps --cluster-id $CLUSTER_ID | \
    python get_json_field.py --pretty Steps

echo "~~~ Failed Steps: ~~~"
aws emr list-steps --cluster-id $CLUSTER_ID --step-states FAILED | \
    python get_json_field.py --pretty Steps

echo "Finished run on dataset s3://itamaro/$DATASET at $(date)"
echo "To terminate the cluster, run:"
echo aws emr terminate-clusters --cluster-ids $CLUSTER_ID
