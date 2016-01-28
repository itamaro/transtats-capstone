#!/bin/bash

usage_and_die() {
  echo "Usage: $0 [local_dataset] [output_dir]" >&2
  exit 42
}

run_test() {
  Q="$1"
  shift
  TEST="$1"
  shift
  echo "Running local test for $Q"
  local start="$( date +%s )"
  python "$TEST_DIR/${TEST}.py" "$DATA_DIR" "$@" > "$OUTPUT_DIR/$Q"
  echo "$Q produced $( cat "$OUTPUT_DIR/$Q" | wc -l ) output rows" \
       "in $(( $( date +%s ) - start )) seconds, exit code $?"
}

DATA_DIR="$1"
OUTPUT_DIR="$2"
TEST_DIR="$( cd $( dirname $0 ) && pwd )"

if [ ! -d "$DATA_DIR" ]; then
  usage_and_die
fi

if [ ! -d "$OUTPUT_DIR" ]; then
  usage_and_die
fi

echo "Starting at $(date)"

echo "Running on local dataset: $( ls $DATA_DIR | xargs )" \
     "($( cat $DATA_DIR/* | wc -l ) rows)"

echo "Clearing out output directory $OUTPUT_DIR"
rm $OUTPUT_DIR/* &> /dev/null

# Group 1 questions
run_test g1q1 count_flights_all_csvs &
run_test g1q2 rank_ontime_perf_all_csvs --key airline --reverse --delay arr &
run_test g1q3 rank_ontime_perf_all_csvs --key dow     --reverse --delay arr &
wait

# Group 2 questions
run_test g2q1 rank_ontime_perf_for_airport_all_csvs --key airline --reverse --delay dep -n 10 &
run_test g2q2 rank_ontime_perf_for_airport_all_csvs --key dest    --reverse --delay dep -n 10 &
run_test g2q4 average_delay --delay arr --key dest &
wait

# Group 3 questions
run_test g3q1 test_zipf_dist
# no manual test for Tom's challenge

echo "Finished at $(date)"
