#!/bin/bash
DATA_BASE_DIR="/mnt/data/aviation/airline_ontime"
echo "0" > "totsize"
find $DATA_BASE_DIR -iname "*.zip" -type f -exec bash etl_one_zip.sh {} \;
