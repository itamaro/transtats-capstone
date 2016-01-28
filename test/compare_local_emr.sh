#!/bin/bash

usage_and_die() {
  echo "Usage: $0 [local_output_dir] [s3_output_dir]" >&2
  exit 42
}

compare_question() {
  Q="$1"
  EMR_OUT="$S3_LOCAL_COPY/$Q"
  mkdir "$EMR_OUT"
  aws s3 cp "$S3_OUTPUT_DIR/$Q" "$EMR_OUT" --recursive > /dev/null
  TEST_OUT="$OUTPUT_DIR/$Q"
  if [ "$Q" == "g1q2" ]; then
    CAT="tail -10"
  else
    CAT="cat"
  fi
  DIFF_FILE="$( mktemp )"
  diff <($CAT $TEST_OUT | sort) <(cat $EMR_OUT/* | sort) > "$DIFF_FILE"
  if [ "$( cat "$DIFF_FILE" | wc -l )" -gt 0 ]; then
    if [ "$Q" == "g3q1" ]; then
      echo "Pearson's correlation coefficient from local test:"
      cat "$TEST_OUT"
      echo "Pearson's correlation coefficient from EMR:"
      cat $EMR_OUT/*
    else
      echo "~~~~ Diff for $Q ~~~~"
      cat "$DIFF_FILE"
    fi
  else
    echo "No diff for $Q"
  fi
}

OUTPUT_DIR="$1"
S3_OUTPUT_DIR="$2"

if [ ! -d "$OUTPUT_DIR" ]; then
  usage_and_die
fi

if [[ ! $S3_OUTPUT_DIR =~ s3:// ]]; then
  usage_and_die
fi

echo "Comparing local outputs in $OUTPUT_DIR with EMR outputs in $S3_OUTPUT_DIR"

S3_LOCAL_COPY="$( mktemp -d )"
for q in $( ls $OUTPUT_DIR ); do
  compare_question $q
done

rm -r "$S3_LOCAL_COPY"
