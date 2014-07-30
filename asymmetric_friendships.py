import MapReduce
import sys

"""

"""

mr = MapReduce.MapReduce()

friendships = {}
def mapper(relation):	# [friendA, friendB]
    # key: friendA
    # value: friendB
    friendA = relation[0] #first friend
    friendB = relation[1] #second friend
    global friendships 
    friendships[(friendA, friendB)] = 1
    #friendships[(friendB, friendA)] = 0
    mr.emit_intermediate(friendA, friendB)

def reducer(friendA, list_of_friends):
    # key: friendA
    # value: list of incident edges to friendA node
    for friendB in list_of_friends:
      if ((friendB, friendA) not in friendships.keys()):
        mr.emit((friendA, friendB))
	mr.emit((friendB, friendA))


if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

