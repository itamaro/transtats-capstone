#!/usr/bin/python2.7

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


def etl_dir(in_dir, out_dir, *args):
  for root, dirs, files in os.walk(in_dir):
    for filename in files:
      if filename.lower().endswith('.csv'):
        fullpath = os.path.join(root, filename)
        out_csvpath = os.path.join(out_dir, filename)
        with open(fullpath, 'rb') as csvfile:
          with open(out_csvpath, 'wb') as out_csvfile:
            writer = csv.DictWriter(out_csvfile, fieldnames=FIELDS)
            writer.writeheader()
            for row in csv.DictReader(csvfile):
              filtered_row = {field: row[field] for field in FIELDS}
              for value in filtered_row.itervalues():
                if not value:
                  # Skipping bad row
                  break
              else:  # if didn't break from the for
                writer.writerow(filtered_row)
        csvsize = os.path.getsize(out_csvpath)
        print 'filtered', filename, 'size is', csvsize
        accumulate_size(csvsize)


if '__main__' == __name__:
  etl_dir(*sys.argv[1:])
