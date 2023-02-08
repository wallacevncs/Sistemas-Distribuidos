import sys, os, time, math, re
from pyDF import *

sys.path.append(os.environ['PYDFHOME'])
	
regex = '^\(?(?:[14689][1-9]|2[12478]|3[1234578]|5[1345]|7[134579])\)? ?(?:[2-8]|9[1-9])[0-9]{3}\-?[0-9]{4}$'

def filterPhones(args):
	
    sp = args[0]

    if re.search(regex, sp):

        return sp[:-1]

    else:

        return ""

def printPhones(args):
	
    if args[0] != "":
		
        print args[0]


nprocs = int(sys.argv[1])
filename = sys.argv[2]

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

fp = open(filename, "r")

src = Source(fp)
graph.add(src)

nd = FilterTagged(filterPhones, 1)
graph.add(nd)

ser = Serializer(printPhones, 1)
graph.add(ser)


src.add_edge(nd, 0)
nd.add_edge(ser, 0)


t0 = time.time()
sched.start()
t1 = time.time()

print "Execution time %.3f" %(t1-t0)
