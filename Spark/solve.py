#!/usr/bin/python

import argparse
import csv
from datetime import datetime, timedelta
from operator import add, itemgetter
from os.path import join
from pyspark import SparkContext
from StringIO import StringIO


FIELDS = ['DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin',
          'Dest', 'DepTime', 'DepDelay', 'ArrTime', 'ArrDelay', 'Flights']


def parse_csv_record(line):
  return csv.DictReader(StringIO(line), fieldnames=FIELDS).next()


##### GROUP 1 QUESTIONS #####


def airport_count_flights(record):
  try:
    flights = float(record['Flights'])
    if flights > 0:
      yield (record['Origin'], flights)
      yield (record['Dest'], flights)
  except:
    # probably reading a header line again
    return


def solve_g1q1(input_rdd, args):
  airport_to_flight_count_rdd = (input_rdd.flatMap(airport_count_flights)
                                 .reduceByKey(add))
  top_airports = airport_to_flight_count_rdd.top(10, key=itemgetter(1))
  sc.parallelize(
      '%s\t%d' % (airport, int(num_flights))
      for airport, num_flights in top_airports).saveAsTextFile(join(args.output_dir, 'g1q1/'))


def get_delay(key, delay_field, per_origin=False):
  def delay_for_key(record):
    try:
      flights = float(record['Flights'])
      if flights > 0 and record[delay_field]:
        delay = float(record[delay_field])
        if delay > 0:  # ignoring early arrival / departure
          # multiplying the score by number of flights (weight factor)
          out_key = (record['Origin'], record[key]) if per_origin else record[key]
          yield (out_key, (delay * flights, int(flights)))
    except:
      return
  return delay_for_key


def sum_val_count(val_count1, val_count2):
  return val_count1[0] + val_count2[0], val_count1[1] + val_count2[1]


def averager(record):
  val, count = record[1]
  return record[0], val / count


def solve_g1q2(input_rdd, args):
  airline_arrdelay_rdd = (input_rdd
                          .flatMap(get_delay('UniqueCarrier', 'ArrDelay'))
                          .reduceByKey(sum_val_count).map(averager))
  top_airlines = airline_arrdelay_rdd.top(10, key=lambda x: -x[1])
  sc.parallelize(
      '%s\t%f' % (airline, delay) for airline, delay in top_airlines
      ).saveAsTextFile(join(args.output_dir, 'g1q2/'))


def solve_g1q3(input_rdd, args):
  dow_arrdelay_rdd = (input_rdd
                      .flatMap(get_delay('DayOfWeek', 'ArrDelay'))
                      .reduceByKey(sum_val_count).map(averager)
                      .sortBy(itemgetter(1))
                      .map(lambda x: '%s\t%f' % (x[0], x[1])))
  dow_arrdelay_rdd.saveAsTextFile(join(args.output_dir, 'g1q3/'))


##### GROUP 2 QUESTIONS #####


def key_value_manip(record):
  return record[0][0], (record[0][1], record[1])


def top10_by_val(tuples_list):
  return sorted(tuples_list, key=itemgetter(1))[:10]


def splitter(record):
  key, values_list = record
  for values in values_list:
    yield key, values[0], values[1]


def hive_formatter(record):
  return '{x[0]}:{x[1]}\x01{x[0]}\x01{x[1]}\x01{x[2]}'.format(x=record)


def solve_g2q1(input_rdd, args):
  airline_depdelay_per_origin_rdd = (
      input_rdd
      .flatMap(get_delay('UniqueCarrier', 'DepDelay', per_origin=True))
      .reduceByKey(sum_val_count).map(averager))
  origin_top_airlines_rdd = (
      airline_depdelay_per_origin_rdd.map(key_value_manip)
      .groupByKey().mapValues(top10_by_val).flatMap(splitter))
  origin_top_airlines_rdd.map(hive_formatter).saveAsTextFile(join(args.hive_dir, 'g2q1/'))


def solve_g2q2(input_rdd, args):
  dest_depdelay_per_origin_rdd = (
      input_rdd
      .flatMap(get_delay('Dest', 'DepDelay', per_origin=True))
      .reduceByKey(sum_val_count).map(averager))
  origin_top_dests_rdd = (
      dest_depdelay_per_origin_rdd.map(key_value_manip)
      .groupByKey().mapValues(top10_by_val).flatMap(splitter))
  origin_top_dests_rdd.map(hive_formatter).saveAsTextFile(join(args.hive_dir, 'g2q2/'))


def solve_g2q4(input_rdd, args):
  def hive_formatter(record):
    return '{x[0][0]}:{x[0][1]}\x01{x[0][0]}\x01{x[0][1]}\x01{x[1]}'.format(x=record)
  mean_arrdelay_rdd = (
      input_rdd
      .flatMap(get_delay('Dest', 'ArrDelay', per_origin=True))
      .reduceByKey(sum_val_count).map(averager))
  mean_arrdelay_rdd.map(hive_formatter).saveAsTextFile(join(args.hive_dir, 'g2q4/'))


##### TOM'S CHALLENGE #####


TOM_YEAR = 2008
LEG1_MIN_DATE = datetime.strptime('2008-01-01', '%Y-%m-%d')
LEG1_MAX_DATE = datetime.strptime('2008-12-29', '%Y-%m-%d')
LEG2_MIN_DATE = datetime.strptime('2008-01-03', '%Y-%m-%d')
LEG2_MAX_DATE = datetime.strptime('2008-12-31', '%Y-%m-%d')


def get_leg_candidate(first_leg=False, second_leg=False):
  def leg_candidate(record):
    try:
      flight_date = datetime.strptime(record['FlightDate'], '%Y-%m-%d')
      if flight_date.year != TOM_YEAR:
        return  # not a Tom year
      dep_time = datetime.strptime(record['DepTime'], '%H%M')
      if first_leg:
        if LEG1_MIN_DATE <= flight_date <= LEG1_MAX_DATE and dep_time.hour < 12:
          key = (record['Origin'], record['Dest'],
                 datetime.strftime(flight_date, '%Y-%m-%d'))
          value = (record['UniqueCarrier'] + '-' + record['FlightNum'],
                   record['DepTime'], float(record['ArrDelay']))
          yield (key, value)
      elif second_leg:
        if LEG2_MIN_DATE <= flight_date <= LEG2_MAX_DATE and dep_time.hour > 12:
          key = (record['Dest'], record['Origin'],
                 datetime.strftime(flight_date + timedelta(days=-2), '%Y-%m-%d'))
          value = (record['UniqueCarrier'] + '-' + record['FlightNum'],
                   record['DepTime'], float(record['ArrDelay']))
          yield (key, value)
      else:
        raise ValueError('No leg is set...')
    except:
      return
  return leg_candidate


def legs_to_route(legs_rec):
  key, values = legs_rec
  y_airport, flight_date = key
  leg1_val, leg2_val = values
  x_airport, leg1_flight, dep_time1, delay1 = leg1_val
  z_airport, leg2_flight, dep_time2, delay2 = leg2_val
  out_key = (flight_date, x_airport, y_airport, z_airport)
  out_value = (leg1_flight, dep_time1, leg2_flight, dep_time2, delay1 + delay2)
  return (out_key, out_value)


def best_route(routes):
  return sorted(routes, key=itemgetter(-1))[0]


def prepare_for_join(record):
  key, value = record
  return (key[1], key[2]), (key[0], value[0], value[1], value[2])


def tom_hive_formatter(record):
  return '\x01'.join([':'.join(record[0]), '\x01'.join(record[0]), '\x01'.join(map(str, record[1]))])


def solve_g3q2(input_rdd, args):
  leg1_candidates_rdd = (input_rdd.flatMap(get_leg_candidate(first_leg=True))
                         .groupByKey().mapValues(best_route).map(prepare_for_join))
  leg2_candidates_rdd = (input_rdd.flatMap(get_leg_candidate(second_leg=True))
                         .groupByKey().mapValues(best_route).map(prepare_for_join))
  tom_candidates_rdd = leg1_candidates_rdd.join(leg2_candidates_rdd)
  tom_routes_rdd = (
      tom_candidates_rdd.map(legs_to_route).groupByKey().mapValues(best_route))
  tom_routes_rdd.map(tom_hive_formatter).saveAsTextFile(join(args.hive_dir, 'g3q2/'))


##### MAIN #####


if '__main__' == __name__:
  parser = argparse.ArgumentParser()
  parser.add_argument('--input_dir', default='/dataset/')
  parser.add_argument('--output_dir', default='/output/')
  parser.add_argument('--hive_dir', default='/hive_input/')
  args = parser.parse_args()
  sc = SparkContext(appName='Coursera Cloud Capstone Task 2')
  input_rdd = sc.textFile(args.input_dir).map(parse_csv_record).cache()
  solve_g1q1(input_rdd, args)
  solve_g1q2(input_rdd, args)
  solve_g1q3(input_rdd, args)
  solve_g2q1(input_rdd, args)
  solve_g2q2(input_rdd, args)
  solve_g2q4(input_rdd, args)
  solve_g3q2(input_rdd, args)
  sc.stop()
