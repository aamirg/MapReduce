import MapReduce
import sys

"""
Friend Count Example in the Simple Python MapReduce Framework
In the map phase we emit each record with a unique key and count of 1. If there
there is a symmetric pair in the file [pA,pB] and [pB,pA] their counts in the
reduce phase will be equal to [(pA,pB),1,1] so we check length of the list of
values if its less than 2 it means its asymmetric relationship as it occurred
only once and we output it.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    personA = record[0]
    personB = record[1]
    mr.emit_intermediate((personA,personB), 1)
    mr.emit_intermediate((personB,personA), 1)


def reducer(key, list_of_values):
    # key: unique key made of (personA,personB)
    # value: list of counts

   if len(list_of_values)<2:
       mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("friends.json","r") #(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
