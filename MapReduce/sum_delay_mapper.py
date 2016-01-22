#!/usr/bin/python

import argparse
import csv
import sys


DELAY_KINDS = {'arr': 'ArrDelay', 'dep': 'DepDelay'}
KEY_FIELD = {'dow': 'DayOfWeek', 'airline': 'UniqueCarrier', 'dest': 'Dest'}


def sum_delay(args):
  """Mapper that yields the on-time performance stat (arrival/departure delay)
     score for the requested key (airline / day-of-week) for every flight
     entry processed.

  A "score" is yielded only if there is a penalty to accrue (meaning there was
  actually a delay). There's no "bonus" (or penalty) for being early.

  The score is linear in the delay, and weighted by the number of flights
  represented by the record being processed.

  The number of flights is also yielded along with the score in order to allow
  chained reducer to compute aveage delay over all flights, so it can be used
  as a normalized score.
  """
  delay_field = DELAY_KINDS[args.delay]
  key = KEY_FIELD[args.key]
  for row in csv.DictReader(sys.stdin):
    try:
      flights = float(row['Flights'])
    except ValueError:
      # probably reading a header line again
      continue
    # throwing away rows with no flights or no on-time performance data
    if flights > 0 and row[delay_field]:
      delay = float(row[delay_field])
      if delay > 0:  # ignoring early arrival / departure
        # multiplying the score by number of flights (weight factor)
        out_key = '%s:%s' % (row['Origin'], row[key]) if args.per_origin else row[key]
        print '%s\t%f\t%d' % (out_key, flights * delay, int(flights))


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('--delay', choices=DELAY_KINDS.keys(), default='arr')
  parser.add_argument('--key', choices=KEY_FIELD.keys(), required=True)
  parser.add_argument('--per_origin', action='store_true')
  args = parser.parse_args()
  sum_delay(args)
