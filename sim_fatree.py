from network import Network, Node, Link, Message
from program import Program

n = Network()

nodes = {}

# Fat tree of infinitely fast nodes
nodes[0] = n.add_node(Node())
nodes[1] = n.add_node(Node())
nodes[2] = n.add_node(Node())
nodes[3] = n.add_node(Node())
nodes[4] = n.add_node(Node())
nodes[5] = n.add_node(Node())
nodes[6] = n.add_node(Node())

n.join_symmetric(nodes[0], nodes[1], 2**10, 0.1)
n.join_symmetric(nodes[0], nodes[2], 2**10, 0.1)
n.join_symmetric(nodes[1], nodes[3], 2**10, 0.1)
n.join_symmetric(nodes[1], nodes[4], 2**10, 0.1)
n.join_symmetric(nodes[2], nodes[5], 2**10, 0.1)
n.join_symmetric(nodes[2], nodes[6], 2**10, 0.1)

n.initialize_routes()
print str(n)
# point-to-point
block = []

for i in range(1,2):
    block += [n.inject(Message(nodes[0],nodes[i],1024))]

end_time = n.run()
print "Simulation took", end_time





n.reset()

# Set up the program
p = Program()
m = None
# for i in range(0, 10):
#     m = p.add(Message(), after=m)

# n.run_program(p)

#topology-aware
# n1 = n.inject(Message(0,1,1024))
# n2 = n.inject(Message(0,2,1024))

# n3 = n.inject(Message(1,3,1024), waitfor=[n1])
# n4 = n.inject(Message(1,4,1024), waitfor=[n1])
# n5 = n.inject(Message(2,5,1024), waitfor=[n2])
# n6 = n.inject(Message(2,6,1024), waitfor=[n2])

# end_time = n.run()
# print "Simulation took", end_time