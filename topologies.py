from network import Network, Node, Edge


def torus2d(x,y):
    n = Network()

    nodes = {}

    for i in range(y):
        nodes[i] = {}
        for j in range(x):
            nodes[i][j] = n.add_node(Node(float('inf'), 0.0))

    for i in range(1,y-1):
        for j in range(1,x-1):
            n.join(nodes[j][i], nodes[j][i+1], Edge(2**10))
            n.join(nodes[j][i], nodes[j][i-1], Edge(2**10))
            n.join(nodes[j][i], nodes[j+1][i], Edge(2**10))
            n.join(nodes[j][i], nodes[j-1][i], Edge(2**10))

    for i in range(x):
        n.join(nodes[0][i], nodes[y-1][i], Edge(2**10))
        n.join(nodes[0][i], nodes[1][i], Edge(2**10))
        n.join(nodes[y-1][i], nodes[y-2][i], Edge(2**10))
    
    for i in range(y):
        n.join(nodes[i][0], nodes[i][x-1], Edge(2**10))
        n.join(nodes[i][0], nodes[i][1], Edge(2**10))
        n.join(nodes[i][x-1], nodes[i][x-2], Edge(2**10))

    return n