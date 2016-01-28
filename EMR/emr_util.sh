#!/bin/bash

wait_for_ready_cluster() {
  cluster_id="$1"
  # A short sleep before starting to wait, so the cluster has time to start
  # running any pending steps, and we don't wrongly assumes it's idle...
  sleep 30
  while :
  do
    state="$(aws emr describe-cluster --cluster-id $cluster_id | \
             python get_json_field.py Cluster Status State)"
    if [ "$state" == "WAITING" ]; then
      return 0
    fi
    echo "Current cluster state: $state - Sleeping for 60 seconds ..."
    sleep 60
  done
}
