#!/usr/bin/python2.7

from cStringIO import StringIO
import csv
import os
import sys


ACC_FILE = 'totsize'

# commenting out fields that I don't want to keep
FIELDS = [
    # 'Year',
    # 'Quarter',
    # 'Month',
    # 'DayofMonth',
    'DayOfWeek',  # For G1-Q3
    'FlightDate',
    'UniqueCarrier',
    # 'AirlineID',
    # 'Carrier',
    # 'TailNum',
    'FlightNum',  # For Tom
    'Origin',
    # 'OriginCityName',
    # 'OriginState',
    # 'OriginStateFips',
    # 'OriginStateName',
    # 'OriginWac',
    'Dest',
    # 'DestCityName',
    # 'DestState',
    # 'DestStateFips',
    # 'DestStateName',
    # 'DestWac',
    # 'CRSDepTime',
    'DepTime',  # For Tom
    'DepDelay',
    # 'DepDelayMinutes',
    # 'DepDel15',
    # 'DepartureDelayGroups',
    # 'DepTimeBlk',
    # 'TaxiOut',
    # 'WheelsOff',
    # 'WheelsOn',
    # 'TaxiIn',
    # 'CRSArrTime',
    'ArrTime',  # For Tom
    'ArrDelay',
    # 'ArrDelayMinutes',
    # 'ArrDel15',
    # 'ArrivalDelayGroups',
    # 'ArrTimeBlk',
    # 'Cancelled',
    # 'CancellationCode',
    # 'Diverted',
    # 'CRSElapsedTime',
    # 'ActualElapsedTime',
    # 'AirTime',
    'Flights',
    # 'Distance',
    # 'DistanceGroup',
    # 'CarrierDelay',
    # 'WeatherDelay',
    # 'NASDelay',
    # 'SecurityDelay',
    # 'LateAircraftDelay',
]


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
        with open(fullpath, 'rb') as csvfile:
          out_csv_buff = StringIO()
          writer = csv.DictWriter(out_csv_buff, fieldnames=FIELDS)
          writer.writeheader()
          for row in csv.DictReader(csvfile):
            writer.writerow({field: row[field] for field in FIELDS})
        csvsize = len(out_csv_buff.getvalue())
        out_csv_buff.close()
        del out_csv_buff
        print 'filtered', filename, 'size is', csvsize
        accumulate_size(csvsize)


if '__main__' == __name__:
  etl_dir(*sys.argv[1:])
