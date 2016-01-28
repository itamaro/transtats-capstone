#!/usr/bin/python

import json
from pprint import pprint
import sys

if '__main__' == __name__:
  if len(sys.argv) > 1:
    all_args = sys.argv[1:]
    if all_args[0] == '--pretty':
      pretty = True
      all_args = all_args[1:]
    else:
      pretty = False
  obj = json.load(sys.stdin)
  for arg in all_args:
    obj = obj[arg]
  if pretty:
    pprint(obj)
  else:
    print obj
