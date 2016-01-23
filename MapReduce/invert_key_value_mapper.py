#!/usr/bin/python

import sys

if '__main__' == __name__:
  for line in sys.stdin:
    print '\t'.join(reversed(line.split()))
