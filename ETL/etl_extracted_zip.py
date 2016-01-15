#!/usr/bin/python2.7

import os
import sys


ACC_FILE = 'totsize'


def accumulate_size(size):
  with open(ACC_FILE, 'r') as acc_file:
    start_size = int(acc_file.read().strip())
  new_size = start_size + size
  with open(ACC_FILE, 'w') as acc_file:
    acc_file.write(str(new_size))
  print 'Total size so far is', new_size


def etl_dir(base_dir, *args):
  for root, dirs, files in os.walk(base_dir):
    for filename in files:
      if filename.lower().endswith('.csv'):
        fullpath = os.path.join(root, filename)
        csvsize = os.path.getsize(fullpath)
        print filename, 'size is', csvsize
        accumulate_size(csvsize)


if '__main__' == __name__:
  etl_dir(*sys.argv[1:])
