#!/usr/bin/python

import math
import sys


def zipf_corr_coef():
  """Reducer for checking fit of data to Zipf distribution using Pearson's
     Correlation Coefficient.

  The expected input is a frequencies list of the data that we're fitting,
  ordered from highest to lowest frequency (e.g. by "rank").
  Since Pearson's-r computation requires processing all of the rank-frequency
  data, this must be executed as a single reducer, with the frequencies as the
  key (the value is ignored), sorted by the key as a numeric value, in reverse
  order (from highest to lowest).

  In Hadoop, this sorting can be obtained using these options:
  mapred.output.key.comparator.class="org.apache.hadoop.mapred.lib.KeyFieldBasedComparator"
  mapreduce.partition.keycomparator.options="-k1nr"
  ("k1" means use first field as key, "n" means treat it as numeric value and
  not plain text, "r" means sort in reverse order).
  """
  rank = 0
  current_freq = None
  # For computing the Pearson correlation coefficient,
  # x is log(rank), and y is log(frequency).
  num_values = 0
  sig_xy = sig_x = sig_y = sig_xx = sig_yy = 0.0

  for line in sys.stdin:
    freq_str = line.split('\t')[0]
    try:
      freq = float(freq_str)
    except ValueError:
      sys.stderr.write('WTF %s\n' % (line))
      continue
    if freq != current_freq:
      rank, current_freq = rank + 1, freq
    x = math.log(rank)
    y = math.log(freq)
    num_values += 1
    sig_x, sig_y = sig_x + x, sig_y + y
    sig_xx, sig_yy, sig_xy = sig_xx + x ** 2, sig_yy + y ** 2, sig_xy + x * y

  # Finished consuming data - compute and yield the resulting Pearson's-r
  r_xy = (float(num_values * sig_xy - sig_x * sig_y) /
          (math.sqrt(num_values * sig_xx - sig_x ** 2) *
           math.sqrt(num_values * sig_yy - sig_y ** 2)))
  print 'pearson_r\t%f' % (r_xy)


if '__main__' == __name__:
  zipf_corr_coef()
