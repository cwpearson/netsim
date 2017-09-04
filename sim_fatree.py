from network import Network, Node, Link, Message

n = Network()

# Fat tree of infinitely fast nodes
n0 = n.add_node(Node())
n1 = n.add_node(Node())
n2 = n.add_node(Node())
n3 = n.add_node(Node())
n4 = n.add_node(Node())
n5 = n.add_node(Node())
n6 = n.add_node(Node())

n.join_symmetric(n0, n1, 2**10, 0.1)
n.join_symmetric(n0, n2, 2**10, 0.1)
n.join_symmetric(n1, n3, 2**10, 0.1)
n.join_symmetric(n1, n4, 2**10, 0.1)
n.join_symmetric(n2, n5, 2**10, 0.1)
n.join_symmetric(n2, n6, 2**10, 0.1)

# point-to-point
block = []
for i in range(3,7):
    block += [n.inject(Message(0,i,1024))]

end_time = n.run()
print "Simulation took", end_time


n.reset()

#topology-aware
n1 = n.inject(Message(0,1,1024))
n2 = n.inject(Message(0,2,1024))

n3 = n.inject(Message(1,3,1024), waitfor=[n1])
n4 = n.inject(Message(1,4,1024), waitfor=[n1])
n5 = n.inject(Message(2,5,1024), waitfor=[n2])
n6 = n.inject(Message(2,6,1024), waitfor=[n2])

end_time = n.run()
print "Simulation took", end_time