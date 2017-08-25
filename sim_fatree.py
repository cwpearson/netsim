from network import Network, Node, Edge, Message

n = Network()

# Fat tree of infinitely fast nodes
n0 = n.add_node(Node(float('inf'), 0.1))
n1 = n.add_node(Node(float('inf'), 0.1))
n2 = n.add_node(Node(float('inf'), 0.1))
n3 = n.add_node(Node(float('inf'), 0.1))
n4 = n.add_node(Node(float('inf'), 0.1))
n5 = n.add_node(Node(float('inf'), 0.1))
n6 = n.add_node(Node(float('inf'), 0.1))

n.join(n0, n1, Edge(2**10))
n.join(n0, n2, Edge(2**10))
n.join(n1, n3, Edge(2**10))
n.join(n1, n4, Edge(2**10))
n.join(n2, n5, Edge(2**10))
n.join(n2, n6, Edge(2**10))

# point-to-point
block = []
for i in range(1, 2):
    block += [n.inject(Message(0, i, 1024))]

end_time = n.run()
print "Simulation took", end_time


# n.reset()

# # topology-aware
# n1 = n.inject(Message(0, 1, 1024))
# n2 = n.inject(Message(0, 2, 1024))

# n3 = n.inject(Message(1, 3, 1024), waitfor=[n1])
# n4 = n.inject(Message(1, 4, 1024), waitfor=[n1])
# n5 = n.inject(Message(2, 5, 1024), waitfor=[n2])
# n6 = n.inject(Message(2, 6, 1024), waitfor=[n2])

# end_time = n.run()
# print "Simulation took", end_time
