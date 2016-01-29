#!/usr/bin/python

import sys

if '__main__' == __name__:
  for line in sys.stdin:
    parts = line.strip().split('\t')
    key_parts = parts[0].strip().split(':')
    print '\x01'.join(parts[:1] + key_parts + parts[1:])
