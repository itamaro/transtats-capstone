#/bin/bash

S3_DIR="s3://itamaro/emr.dataset/"
KFK_TOPIC="trans-data"

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
    --partitions 1 --topic "$KFK_TOPIC"

for CSV in $( aws s3 ls "$S3_DIR" | awk '{print $4}' | tac ); do
  S3_FILE="${S3_DIR}${CSV}"
  echo "Publishing $S3_FILE from S3 to Kafka topic $KFK_TOPIC"
  aws s3 cp "$S3_FILE" - |  \
      kafka-console-producer.sh --broker-list localhost:9092 --topic "$KFK_TOPIC"
done
