from network import Network, Node, Link, Message
from program import Program

n = Network()

nodes = {}

# Two nodes
nodes[0] = n.add_node(Node())
nodes[1] = n.add_node(Node())

# Connected with a link
n.join_symmetric(nodes[0], nodes[1], 2**10, 0.1)

# Set up node routes
n.initialize_routes()

# Print network
print str(n)


# Set up the program
p = Program()
m = p.add(Message(nodes[0], nodes[1], 1024))
m = p.add(Message(nodes[0], nodes[1], 1024), after=set([m]))

end_time = n.run_program(p)
print "Sim took", end_time
