from network import Network, Node, Edge, Message

n = Network()

# Fat tree
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
n.join(n2, n5, Edge(2**19))
n.join(n2, n6, Edge(2**19))


h = n.inject(Message(3, 4, 1024))
h = n.inject(Message(5, 6, 1024))
h = n.inject(Message(3, 6, 1024))

## Ping-pong a bit
h = h.inject(Message(6,3,1024))
h = h.inject(Message(3,6,1024))
h = h.inject(Message(6,3,1024))
h = h.inject(Message(3,6,1024))
h = h.inject(Message(6,3,1024))
h = h.inject(Message(3,6,1024))

end_time = n.run()
print "Simulation took", end_time