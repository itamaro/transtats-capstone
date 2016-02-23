#!/usr/bin/python

"""
Run with:
spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 streamsolve.py 2> /dev/null

Watch output dir with:
while : ; do clear; ls -ltr ../spark_out/ | tail -6; sleep 5; done
"""

from StringIO import StringIO
import csv
from datetime import datetime, timedelta
from operator import add, itemgetter
from os.path import join
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


SC = SparkContext(appName='Coursera Cloud Capstone Task 2')
SSC = StreamingContext(SC, 300)
BASE_DIR = '/stream'
CHKP_DIR = join(BASE_DIR, 'chkp')
OUTPUT_DIR = join(BASE_DIR, 'spark_out')
SSC.checkpoint(CHKP_DIR)
FIELDS = ['DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin',
          'Dest', 'DepTime', 'DepDelay', 'ArrTime', 'ArrDelay', 'Flights']


def parse_csv(streaming_line):
  _, line = streaming_line
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


def sum_flights(new_values, running_count):
  if running_count:
    return running_count + sum(new_values)
  return sum(new_values)


def solve_g1q1(input_dstream):
  airport_to_flight_count_dstream = (input_dstream
                                     .flatMap(airport_count_flights)
                                     .reduceByKey(add))
  running_count_dstream = airport_to_flight_count_dstream.updateStateByKey(sum_flights)
  running_count_dstream.transform(
      lambda rdd: rdd.sortBy(itemgetter(1), ascending=False)).pprint()


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


def sum_delays(new_values, running_delay_count):
  if running_delay_count is None:
    running_delay_count = (0, 0)
  return (running_delay_count[0] + sum(map(itemgetter(0), new_values)),
          running_delay_count[1] + sum(map(itemgetter(1), new_values)))


def solve_g1q2(streaming_input):
  airline_arrdelay_dstream = (streaming_input
                              .flatMap(get_delay('UniqueCarrier', 'ArrDelay'))
                              .reduceByKey(sum_val_count))
  running_arrdelays_dstream = airline_arrdelay_dstream.updateStateByKey(sum_delays)
  average_arrdelays_dstream = running_arrdelays_dstream.map(averager)
  sorted_delays_dstream = average_arrdelays_dstream.transform(
      lambda rdd: rdd.sortBy(itemgetter(1)))
  sorted_delays_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g1q2'))


def solve_g1q3(streaming_input):
  dow_arrdelay_dstream = (streaming_input
                          .flatMap(get_delay('DayOfWeek', 'ArrDelay'))
                          .reduceByKey(sum_val_count))
  running_arrdelays_dstream = dow_arrdelay_dstream.updateStateByKey(sum_delays)
  average_arrdelays_dstream = running_arrdelays_dstream.map(averager)
  sorted_delays_dstream = average_arrdelays_dstream.transform(
      lambda rdd: rdd.sortBy(itemgetter(1)))
  sorted_delays_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g1q3'))


##### GROUP 2 QUESTIONS #####


def key_value_manip(record):
  return record[0][0], (record[0][1], record[1])


def top10_by_val(tuples_list):
  return sorted(tuples_list, key=itemgetter(1))[:10]


def splitter(record):
  key, values_list = record
  for values in values_list:
    yield key, values[0], values[1]


def solve_g2q1(streaming_input):
  airline_depdelay_per_origin_dstream = (
      streaming_input
      .flatMap(get_delay('UniqueCarrier', 'DepDelay', per_origin=True))
      .reduceByKey(sum_val_count))
  running_delay_count_dstream = (
      airline_depdelay_per_origin_dstream.updateStateByKey(sum_delays))
  origin_top_airlines_dstream = (
      running_delay_count_dstream.map(averager).map(key_value_manip)
      .groupByKey().mapValues(top10_by_val).flatMap(splitter))
  origin_top_airlines_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g2q1'))


def solve_g2q2(streaming_input):
  dest_depdelay_per_origin_dstream = (
      streaming_input
      .flatMap(get_delay('Dest', 'DepDelay', per_origin=True))
      .reduceByKey(sum_val_count))
  running_delay_count_dstream = (
      dest_depdelay_per_origin_dstream.updateStateByKey(sum_delays))
  origin_top_dest_dstream = (
      running_delay_count_dstream.map(averager).map(key_value_manip)
      .groupByKey().mapValues(top10_by_val).flatMap(splitter))
  origin_top_dest_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g2q2'))


def solve_g2q4(streaming_input):
  mean_arrdelay_dstream = (
      streaming_input
      .flatMap(get_delay('Dest', 'ArrDelay', per_origin=True))
      .reduceByKey(sum_val_count))
  running_arrdelays_dstream = mean_arrdelay_dstream.updateStateByKey(sum_delays)
  average_arrdelays_dstream = running_arrdelays_dstream.map(averager)
  average_arrdelays_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g2q4'))


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


def aggregate_candidates_in_state(new_values, running_state):
  if running_state:
    return best_route([running_state] + new_values)
  return best_route(new_values)


def prepare_for_join(record):
  key, value = record
  return (key[1], key[2]), (key[0], value[0], value[1], value[2])


def solve_g3q2(streaming_input):
  leg1_candidates_dstream = (streaming_input
                             .flatMap(get_leg_candidate(first_leg=True))
                             .updateStateByKey(aggregate_candidates_in_state)
                             .map(prepare_for_join))
  leg2_candidates_dstream = (streaming_input
                             .flatMap(get_leg_candidate(second_leg=True))
                             .updateStateByKey(aggregate_candidates_in_state)
                             .map(prepare_for_join))
  tom_routes_dstream = (leg1_candidates_dstream.join(leg2_candidates_dstream)
                        .map(legs_to_route).groupByKey().mapValues(best_route))
  tom_routes_dstream.saveAsTextFiles(join(OUTPUT_DIR, 'g3q2'))


##### MAIN #####


if '__main__' == __name__:
  streaming_input = KafkaUtils.createDirectStream(
      SSC, ['trans-data'],
      {
          'metadata.broker.list': 'ip-10-232-141-24.ec2.internal:9092,ip-10-33-178-125.ec2.internal:9092',
          'auto.offset.reset': 'smallest',
      }).map(parse_csv)
  solve_g1q1(streaming_input)
  solve_g1q2(streaming_input)
  solve_g1q3(streaming_input)
  solve_g2q1(streaming_input)
  solve_g2q2(streaming_input)
  solve_g2q4(streaming_input)
  solve_g3q2(streaming_input)
  SSC.start()
  SSC.awaitTermination()
