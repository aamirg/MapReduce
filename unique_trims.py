import MapReduce
import sys

"""
Unique trims Example in the Simple Python MapReduce Framework
In the map phase we emit each record with a dna key after trimming it by 10. In the second phase we just print the key.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
     value= record[1]
     x=len(value)-10
     dnastring=value[:x]
     mr.emit_intermediate(dnastring,1)

def reducer(key, list_of_values):
    # key: dna string trimmed
    # value: list of counts
       mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("dna.json","r") #(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
