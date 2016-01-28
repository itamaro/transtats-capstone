#!/usr/bin/python

import argparse
import jinja2

if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('template')
  parser.add_argument('-v', '--var', action='append')
  args = parser.parse_args()
  with open(args.template, 'r') as tmpl_f:
    template = jinja2.Template(tmpl_f.read())
    print template.render(dict(var.split('=') for var in args.var))
