
import MapReduce
import sys

"""
Friend Count Example in the Simple Python MapReduce Framework
For each record we emit the count alongwith the name of the first person.
In the reduce phase all the keys are grouped and the total count is calculated by adding the values
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: personA from record
    # value: personB from record
    key = record[0]
    combiner(key,1)


def combiner(key,value):
    friends={}
    if key in friends:
        friends[key]+=1
    else:
        friends[key]=1

    for a in friends:
        mr.emit_intermediate(a, friends[a])


def reducer(key, list_of_values):
    # key: personA
    # value: list of occurrence
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("friends.json","r") #(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
