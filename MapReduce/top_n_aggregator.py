#!/usr/bin/python

import argparse
from collections import defaultdict
import sys


KEYS = {}
KEYSPACES = defaultdict(dict)


def top_values(args):
  for line in sys.stdin:
    key, value_str = line.split()
    value = float(value_str)
    if args.keyspace:
      keyspace, key = key.split(args.keyspace)
      keys_dict = KEYSPACES[keyspace]
    else:
      keys_dict = KEYS
    keys_dict[key] = value
    if len(keys_dict) > args.top_n:
      removed = [keys_dict.pop(rem_key) for rem_key in
                 sorted(keys_dict, key=keys_dict.get, reverse=args.reverse)[:-args.top_n]]
  # Finished - now yield the top N
  def print_keyspace(keys_dict, keyspace=None):
    for key in sorted(keys_dict, key=keys_dict.get, reverse=args.reverse):
      print '%s\t%f' % ('%s:%s' % (keyspace, key) if keyspace else key, keys_dict[key])
  if args.keyspace:
    for keyspace, keys_dict in KEYSPACES.iteritems():
      print_keyspace(keys_dict, keyspace=keyspace)
  else:
    print_keyspace(KEYS)


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--top_n', type=int, default=10)
  parser.add_argument('--reverse', action='store_true')
  parser.add_argument('--keyspace')
  args = parser.parse_args()
  top_values(args)
