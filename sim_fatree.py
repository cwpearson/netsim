from network import Network, Node, Edge, Message

import pubsub as ps

n = Network()

# Fat tree of infinitely fast nodes
n0 = n.add_node(Node(float('inf'), 0.0))
n1 = n.add_node(Node(float('inf'), 0.0))
n2 = n.add_node(Node(float('inf'), 0.0))
n3 = n.add_node(Node(float('inf'), 0.0))
n4 = n.add_node(Node(float('inf'), 0.0))
n5 = n.add_node(Node(float('inf'), 0.0))
n6 = n.add_node(Node(float('inf'), 0.0))

n.join(n0, n1, Edge(2**21))
n.join(n0, n2, Edge(2**21))
n.join(n1, n3, Edge(2**20))
n.join(n1, n4, Edge(2**20))
n.join(n2, n5, Edge(2**20))
n.join(n2, n6, Edge(2**20))

h = n.inject(Message(5, 6, 1024))
h = n.inject(Message(3, 6, 1024))

for i in range(0,5):
    h = n.inject(Message(3,6,1024), waitfor=[h], delay=0.01)

h = n.inject(Message(3,6,1024), waitfor=[h], delay=0.01)

print len(n.pending)

end_time = n.run()
print "Simulation took", end_time