import MapReduce
import sys

"""
Inverted Index Example in the Simple Python MapReduce Framework :
    We take as input doc-id and its text. the text is split into words and from the
map phase we then emit the word alongwith its doc-id. The controller will sort
and group the values by keys. In the reduce phase we use a for loop to eliminate
duplicate entries and we emit the word alongwith the doc-id where it appears.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents text

    key=record[0]
    value=record[1]
    words=value.split()

    for w in words:
        mr.emit_intermediate(w,key)


def reducer(key, list_of_values):
    # key: word
    # value: list of doc-ids
    value=[]
    for v in list_of_values:
        if v not in value:
            value.append(v)
    mr.emit((key,value))


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("books.json","r") #(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
