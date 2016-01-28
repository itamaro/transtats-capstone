#!/usr/bin/python

import sys


def average_values_by_key():
  key = current_key = None
  for line in sys.stdin:
    key, value_str, count_str = line.split()
    if key != current_key:
      if current_key:
        print '%s\t%f' % (current_key, sum_values / sum_counts)
      current_key = key
      sum_values = 0.0
      sum_counts = 0
    sum_values += float(value_str)
    sum_counts += int(count_str)
  # Finished processing input - yield final key
  if key:
    print '%s\t%f' % (key, sum_values / sum_counts)


if '__main__' == __name__:
  average_values_by_key()
