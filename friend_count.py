import MapReduce
import sys

"""
Friend count where input is list of (friendA, friendB) pairs
$ python friend_count.py input_json/friends.json

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: friend A
    # value: friend B
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += 1
    mr.emit((key, total))


if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

