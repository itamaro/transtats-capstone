#!/usr/bin/python

from collections import defaultdict
import sys


MAX_KEYS = 3
VALUES = defaultdict(float)
COUNTS = defaultdict(int)


def average_values_by_key():
  for line in sys.stdin:
    key, value_str, count_str = line.split()
    VALUES[key] += float(value_str)
    COUNTS[key] += int(count_str)
    if len(VALUES) >= MAX_KEYS:
      for done_key in VALUES.keys():
        if done_key != key:
          print '%s\t%f' % (done_key, VALUES.pop(done_key) / COUNTS.pop(done_key))
  # Finished processing input - yield remaining keys
  for done_key in VALUES.keys():
    print '%s\t%f' % (done_key, VALUES.pop(done_key) / COUNTS.pop(done_key))


if '__main__' == __name__:
  average_values_by_key()
