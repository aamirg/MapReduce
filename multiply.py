import MapReduce
import sys

"""
Matrix multiplication Example in the Simple Python MapReduce Framework
Refer slides for logic
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):

    if record[0]=='a':
        for k in range(0,5):
            key=(record[1],k)
            mr.emit_intermediate(key,record)

    if record[0]=='b':
        for i in range(0,5):
            key=(i,record[2])
            mr.emit_intermediate(key,record)

def reducer(key, list_of_values):

    key=list(key)
    total=0
    for j in range(0,5):
        v1=0
        v2=0
        for value in list_of_values:
            if value[0]=='a' and value[2]==j:
                v1=value[3]
            elif value[0]=='b' and value[1]==j:
                 v2=value[3]
                 total+=v1*v2
    key.append(total)
    mr.emit(key)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("matrix.json","r") #(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
