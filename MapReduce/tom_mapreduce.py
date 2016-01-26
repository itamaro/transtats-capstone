#!/usr/bin/python

import argparse
import csv
from datetime import datetime, timedelta
import itertools
import sys


FIELDS = ['DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin',
          'Dest', 'DepTime', 'DepDelay', 'ArrTime', 'ArrDelay', 'Flights']
TOM_YEAR = 2008
LEG1_MIN_DATE = datetime.strptime('2008-01-01', '%Y-%m-%d')
LEG1_MAX_DATE = datetime.strptime('2008-12-29', '%Y-%m-%d')
LEG2_MIN_DATE = datetime.strptime('2008-01-03', '%Y-%m-%d')
LEG2_MAX_DATE = datetime.strptime('2008-12-31', '%Y-%m-%d')


def mapper(args):
  """Process all flight records, emiting just flights that are candidate to be
     first or second leg flights of Tom's X-Y-Z trip.

  Output key: '{Y-airport}:{X-Y-FlightDate}'
  This is chosen to be the output key, because this is the data that is common
  to both the X-Y and the Y-Z legs, allowing me to group these entries together
  when they reach the reducer.

  Output value: '{LegIndicator}:{Airline}-{FlightNum}:{X/Z-airport}:{DepTime}:{ArrDelay}'
  This contains the rest of the data I need for determining Tom's trip, but that
  I don't want to put in the key, because it would harm my grouping.
  """
  for row in csv.DictReader(sys.stdin, fieldnames=FIELDS):
    # Extract flight date/time information, and on-time performance
    try:
      flight_date = datetime.strptime(row['FlightDate'], '%Y-%m-%d')
      if flight_date.year != TOM_YEAR:
        continue  # not a Tom year
      dep_time = datetime.strptime(row['DepTime'], '%H%M')
    except ValueError:
      # probably reading a header line again
      continue

    def write_key_value(leg_indicator):
      if leg_indicator == 'FirstLeg':
        key_airport, value_airport, date_offset = 'Dest', 'Origin', 0
      else:
        key_airport, value_airport, date_offset = 'Origin', 'Dest', -2
      out_key = '%s:%s' % (
          row[key_airport], datetime.strftime(flight_date + timedelta(days=date_offset), '%Y-%m-%d'))
      out_value = '%s:%s-%s:%s:%s:%s' % (
          leg_indicator, row['UniqueCarrier'], row['FlightNum'], row[value_airport], row['DepTime'], row['ArrDelay'])
      print '%s\t%s' % (out_key, out_value)

    # check if the flight can be a first-leg flight
    # requirements:
    # - date range: Jan 1 through Dec 29 2008
    # - departure time: before 12:00 PM local time
    if LEG1_MIN_DATE <= flight_date <= LEG1_MAX_DATE and dep_time.hour < 12:
      # Yes!
      write_key_value('FirstLeg')

    # check if the flight can be a second-leg flight
    # requirements:
    # - date range: Jan 3 through Dec 31 2008
    # - departure time: after 12:00 PM local time
    if LEG2_MIN_DATE <= flight_date <= LEG2_MAX_DATE and dep_time.hour > 12:
      # Yes!
      write_key_value('SecondLeg')


def reducer(args):
  """Process flights proposed for XY-legs and YZ-legs together, choosing those
     with minimal arrival delay as route for every date-X-Y-Z combination.

  I can do this because the mapper key for leg1 and leg2 candidates is identical
  for candidate flights that can be a continuous leg1-leg2 route, so all these
  flights will be fed sequentially to the reducer.
  I just need to find such combinations that have both existing leg1 & leg2,
  and in case of multiple candidate flights per leg, choose the flight with the
  least arrival delay (per leg), since the total arrival delay for the trip is
  independent for each leg.
  """
  current_key = None
  state = {}

  def reset_state():
    for leg in ('FirstLeg', 'SecondLeg'):
      state[leg] = {}

  for line in sys.stdin:
    key, value = line.split()

    if key != current_key:
      # Emit a route, if found one
      if current_key:
        y_airport, xy_flight_date = current_key.split(':')
        leg1, leg2 = state['FirstLeg'], state['SecondLeg']
        for x_airport, z_airport in itertools.product(leg1.iterkeys(), leg2.iterkeys()):
          print '\t'.join([
              xy_flight_date, x_airport, y_airport, z_airport,
              leg1[x_airport]['flight_num'], leg1[x_airport]['flight_dep'],
              leg2[z_airport]['flight_num'], leg2[z_airport]['flight_dep'],
              str(leg1[x_airport]['arr_delay'] + leg2[z_airport]['arr_delay'])])
      reset_state()
      current_key = key

    # Extract packed fields within the value
    leg, flight_num, xz_airport, leg_dep_time, leg_arr_delay_str = value.split(':')
    leg_arr_delay = float(leg_arr_delay_str)
    leg_state = state[leg].setdefault(xz_airport, {'arr_delay': leg_arr_delay,
                                                   'flight_num': flight_num,
                                                   'flight_dep': leg_dep_time})
    if leg_arr_delay < leg_state['arr_delay']:
      leg_state['arr_delay'] = leg_arr_delay
      leg_state['flight_num'] = flight_num
      leg_state['flight_dep'] = leg_dep_time


if '__main__' == __name__:
  handlers = {
      'mapper': mapper,
      'reducer': reducer,
  }
  parser = argparse.ArgumentParser()
  parser.add_argument('func', choices=handlers.keys())
  args = parser.parse_args()
  handlers[args.func](args)
