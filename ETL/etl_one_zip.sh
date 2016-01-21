#!/bin/bash

usage_and_die() {
  echo "$0 [zipfile] [s3_dest_path]" >&2
  exit 42
}

ZIP_FILE="$1"
S3_DIR="$2"

if [ ! -r "$ZIP_FILE" ]; then
  usage_and_die
fi
if [[ ! "$S3_DIR" == s3* ]]; then
  usage_and_die
fi

EXDIR="$(mktemp -d)"
TRANSDIR="$(mktemp -d)"
echo "Extracting $ZIP_FILE to $EXDIR, writing transformed CSV's to $TRANSDIR"
unzip -q "$ZIP_FILE" -d "$EXDIR"
python etl_extracted_zip.py "$EXDIR" "$TRANSDIR"
rm -r "$EXDIR"
echo "Uploading $( ls $TRANSDIR | wc -l ) CSV's to S3 bucket at $S3_DIR: $( ls $TRANSDIR )"
aws s3 cp "$TRANSDIR" "$S3_DIR" --recursive
rm -r "$TRANSDIR"
