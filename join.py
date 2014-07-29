import MapReduce
import sys

"""
Query like: 
SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id
implemented in Simple Python Map Reduce Framework

$ python join.py input_json/records.json
"""

keysB = []
valuesA = []
mr = MapReduce.MapReduce()

def mapper(record): # record could look like ["order", "1" (this is order_id), "36901", "O", ...]
    # key: row identifier
    # value: single table row
    key = record[1]
    value = record
    for item in record:
      mr.emit_intermediate(key, item) 
    # key = '1', value = ('order','1','36901','0',..)
    if (record[0] == "order"):
	keysB.append(record)
    else:
	valuesA.append(record)

def reducer(key, list_of_values):
    # key: word
    # value: row in a table with key as identificator
    global keysB
    global valuesA
    for keyB in keysB:
      for valueA in valuesA:
        if(valueA[1] == keyB[1]):
	  mr.emit((keyB + valueA))
          valuesA.remove(valueA)

if __name__ == '__main__':
  inputdata = open(sys.argv[1]) 
  mr.execute(inputdata, mapper, reducer)
