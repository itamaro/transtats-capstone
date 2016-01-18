#!/bin/bash
ZIP_FILE="$1"
EXDIR="$(mktemp -d)"
TRANSDIR="$(mktemp -d)"
echo "Extracting $ZIP_FILE to $EXDIR, writing transformed CSV's to $TRANSDIR"
unzip -q "$ZIP_FILE" -d "$EXDIR"
python etl_extracted_zip.py "$EXDIR" "$TRANSDIR"
rm -r "$EXDIR"
# TODO: load files in $TRANSDIR to HDFS
rm -r "$TRANSDIR"
