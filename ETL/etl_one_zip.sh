#!/bin/bash
ZIP_FILE="$1"
EXDIR="$(mktemp -d)"
echo "Extracting $ZIP_FILE to $EXDIR"
unzip -q "$ZIP_FILE" -d "$EXDIR"
python etl_extracted_zip.py "$EXDIR"
rm -r "$EXDIR"
