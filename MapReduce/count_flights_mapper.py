#!/usr/bin/python

import csv
import sys


def count_flights():
  for row in csv.DictReader(sys.stdin):
    try:
      flights = float(row['Flights'])
    except ValueError:
      # probably reading a header line again
      continue
    if flights > 0:
      # For using with built-in Hadoop aggregate reducer
      print 'LongValueSum:%s\t%d' % (row['Origin'], flights)
      print 'LongValueSum:%s\t%d' % (row['Dest'], flights)


if '__main__' == __name__:
  count_flights()
