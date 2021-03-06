#!/usr/bin/python2.7

import argparse
from collections import defaultdict
import csv
import os


AIRPORTS = defaultdict(int)


def count_flights(csvfile):
  """Count flights for airports in one CSV file."""
  for row in csv.DictReader(csvfile):
    flights = int(float(row['Flights']))
    if flights > 0:
      AIRPORTS[row['Origin']] += flights
      AIRPORTS[row['Dest']] += flights


def process_all_csvs(args):
  for root, dirs, files in os.walk(args.in_dir):
    for filename in files:
      if filename.lower().endswith('.csv'):
        fullpath = os.path.join(root, filename)
        with open(fullpath, 'rb') as csvfile:
          count_flights(csvfile)


def print_sorted_airports():
  for airport in sorted(AIRPORTS, key=AIRPORTS.get):
    print '%s\t%d' % (airport, AIRPORTS[airport])


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('in_dir')
  args = parser.parse_args()
  process_all_csvs(args)
  print_sorted_airports()
