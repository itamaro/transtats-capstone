#!/bin/bash

wait_for_ready_cluster() {
  cluster_id="$1"
  while :
  do
    state="$(aws emr describe-cluster --cluster-id $cluster_id | \
             python get_json_field.py Cluster Status State)"
    if [ "$state" == "WAITING" ]; then
      return 0
    fi
    echo "Current cluster state: $state - Sleeping for 30 seconds ..."
    sleep 30
  done
}
