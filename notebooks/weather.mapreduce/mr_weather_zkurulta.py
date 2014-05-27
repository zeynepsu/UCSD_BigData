#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRStationWeights(MRJob):
    
  def mapper(self, _, line):
    arr = line.strip().split(',')
    station = arr[0]
    mestype = arr[1]
    year = arr[2]

    data, recs = [], 0
    numdays = 365
    threshold = 1/4
    day = 0
    
    while day < numdays:
      item = arr[3 + day]
      try:
          x = int(item)
      except ValueError:
          x = None
      if x is not None: recs += 1
      data.append(x)
      day=day+1

    if float(recs) / numdays < threshold:
      res = None
    else:
      res = (station, year, mestype, data, recs)
        
    if res is not None:
      station, year, mestype, data, recs = res
      if mestype == 'TMIN' or mestype == 'TMAX':
        yield station, 1

  def combiner(self, key, vals):
    yield key, sum(vals)

  def reducer(self, key, vals):
    yield key, sum(vals)

if __name__ == '__main__':
  MRStationWeights.run()