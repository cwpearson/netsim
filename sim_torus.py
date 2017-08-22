from network import Network, Node, Edge, Message
from topologies import torus2d

n = torus2d(3,3)

# point-to-point
for i in range(1,9):
    n.inject(Message(0,i,1024))


end_time = n.run()
print "Simulation took", end_time