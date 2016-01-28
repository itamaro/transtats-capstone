#!/bin/bash

source emr_util.sh

usage_and_die() {
  echo "Usage: $0 [cluster_id] [stack]" >&2
  exit 42
}

CLUSTER_ID="$1"
STACK="$2"
STEPS_DIR="$( cd $( dirname $0 )/${STACK}-steps && pwd )"

if [[ ! $CLUSTER_ID =~ j-* ]]; then
  usage_and_die
fi

if [ ! -d "$STEPS_DIR" ]; then
  echo "Could not find steps directory at $STEPS_DIR" >&2
  exit 42
fi

echo "Using EMR cluster $CLUSTER_ID to solve questions using $STACK"

echo "Submitting Group-1 jobs"
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps file://$STEPS_DIR/group1.json

echo "Submitting Group-2 jobs"
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps file://$STEPS_DIR/group2.json

echo "Submitting Group-3 jobs"
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps file://$STEPS_DIR/group3.json

wait_for_ready_cluster $CLUSTER_ID
