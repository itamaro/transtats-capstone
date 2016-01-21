#!/bin/bash

usage_and_die() {
  echo "$0 [s3_dest_path]" >&2
  echo "DATA_BASE_DIR must hold a valid readable directory path" >&2
  exit 42
}

DATA_BASE_DIR="/mnt/data/aviation/airline_ontime"
S3_DIR="$1"

if [ ! -d "$DATA_BASE_DIR" ]; then
  usage_and_die
fi
if [[ ! "$S3_DIR" == s3* ]]; then
  usage_and_die
fi

echo "0" > "totsize"
find $DATA_BASE_DIR -iname "*.zip" -type f -exec bash etl_one_zip.sh {} $S3_DIR \;
