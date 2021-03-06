#!/usr/bin/python2.7

import argparse
from collections import defaultdict
import csv
import os


DELAY_KINDS = {'arr': 'ArrDelay', 'dep': 'DepDelay'}
KEY_FIELD = {'dow': 'DayOfWeek', 'airline': 'UniqueCarrier', 'dest': 'Dest'}
AIRPORTS = {}


def rank_ontime_perf(args, csvfile):
  """Rank on-time performance by `args.key` in `csvfile`."""
  delay_field = DELAY_KINDS[args.delay]
  key = KEY_FIELD[args.key]
  for row in csv.DictReader(csvfile):
    flights = float(row['Flights'])
    # throwing away rows with no flights or no on-time performance data
    if flights > 0 and row[delay_field]:
      delay = float(row[delay_field])
      if delay > 0:  # ignoring early arrivals / departures
        # multiplying the score by number of flights (weight factor)
        airport_rec = AIRPORTS.setdefault(
            row['Origin'],
            {'scores': defaultdict(float), 'counts': defaultdict(int)})
        airport_rec['scores'][row[key]] += flights * delay
        airport_rec['counts'][row[key]] += int(flights)


def process_all_csvs(args):
  for root, dirs, files in os.walk(args.in_dir):
    for filename in files:
      if filename.lower().endswith('.csv'):
        fullpath = os.path.join(root, filename)
        with open(fullpath, 'rb') as csvfile:
          rank_ontime_perf(args, csvfile)
  # Normalize all scores by counts
  for airport_rec in AIRPORTS.itervalues():
    for key in airport_rec['scores']:
      assert(key in airport_rec['counts'])
      airport_rec['scores'][key] /= airport_rec['counts'][key]


def print_sorted_scores_for_airports(args):
  for airport, rec in AIRPORTS.iteritems():
    scores = rec['scores']
    for key in sorted(scores, key=scores.get, reverse=args.reverse)[-args.top_n:]:
      print '%s:%s\t%f' % (airport, key, scores[key])


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('in_dir')
  parser.add_argument('--delay', choices=DELAY_KINDS.keys(), default='arr')
  parser.add_argument('--key', choices=KEY_FIELD.keys(), required=True)
  parser.add_argument('-n', '--top_n', type=int, default=0)
  parser.add_argument('--reverse', action='store_true')
  args = parser.parse_args()
  process_all_csvs(args)
  print_sorted_scores_for_airports(args)
