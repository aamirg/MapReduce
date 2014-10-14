Designed and implemented MapReduce algorithms for a variety of common data processing tasks.

MapReduce.py implements the MapReduce programming model, but it executes entirely on a single machine it does not involve parallel computation.

1) inverted_index.py :- Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears.

2)
 friend_count.py :- implements a MapReduce algorithm to count the number of friends for each person

3) friendships.py :- implements a MapReduce algorithm to identify asymmetric friendships. In other words, if [personA, personB] appears
in the list of friends, check whether [personB, personA] is also on the list.If only one of the friendships appears (an asymmetric relationship), emit the pair of names in both orders. If the relationship is symmetric, then produce no output.

4) unique_trims.py:-  removes the last 10 characters from each string of nucleotides, removing any duplicates generated

5) multiply.py :- implements a MapReduce algorithm to compute the matrix multiplication A x B.