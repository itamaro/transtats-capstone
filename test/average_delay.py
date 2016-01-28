#!/usr/bin/python2.7

import argparse
from collections import defaultdict
import csv
import os


DELAY_KINDS = {'arr': 'ArrDelay', 'dep': 'DepDelay'}
KEY_FIELD = {'dow': 'DayOfWeek', 'airline': 'UniqueCarrier', 'dest': 'Dest'}
AIRPORTS = {}


def rank_ontime_perf(args, csvfile):
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
  for origin, airport_rec in AIRPORTS.iteritems():
    for key in airport_rec['scores']:
      assert(key in airport_rec['counts'])
      print '%s:%s\t%f' % (origin, key, airport_rec['scores'][key] / airport_rec['counts'][key])


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('in_dir')
  parser.add_argument('--delay', choices=DELAY_KINDS.keys(), default='arr')
  parser.add_argument('--key', choices=KEY_FIELD.keys(), required=True)
  args = parser.parse_args()
  process_all_csvs(args)
