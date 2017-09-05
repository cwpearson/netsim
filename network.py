import csv
from priorityqueue import PQ 
from program import Program


class UnhandledEventError(Exception):
    pass

class RoutesError(Exception):
    pass

class Uuid(object):
    uuid = 0
    def __init__(self):
        self.uuid_ = self.new_uuid()

    def new_uuid(self):
        Uuid.uuid += 1
        return Uuid.uuid - 1

class Event(object):
    def __init__(self, handler):
        self.handler_ = handler

class EventTxDone(Event):
    def __init__(self, link):
        super(EventTxDone, self).__init__(link)

class EventRecv(Event):
    def __init__(self, node, packet):
        super(EventRecv, self).__init__(node)
        self.packet_ = packet

class Packet(Uuid):
    def __init__(self, message, dst, next_packet, sequence_number, size):
        super(Packet, self).__init__()
        self.dst_ = dst # where this packet is trying to go
        self.link_ = None # Current link being traversed
        self.message_ = message # what message this packet is a part of
        self.next_packet_ = next_packet # next packet in the message
        self.sequence_number_ = sequence_number
        self.size_ = size # size in bytes

class Handler(object):
    def __init__(self):
        pass

    def handle(self, event):
        raise NotImplementedError

class Node(Handler, Uuid):
    def __init__(self):
        self.links_ = {} # link associated with a particular neighbors
        self.network_ = None
        self.route_ = {}  # the next link to take to reach any node
        Uuid.__init__(self)
        Handler.__init__(self)

    def __str__(self):
        return "node"+str(self.uuid_)


    def handle(self, event):
        if isinstance(event, EventRecv):
            self.forward(event.packet_)
        else:
            raise UnhandledEventError(event)

    def forward(self, packet):
        if not self.route_:
            raise RoutesError("node routes are not set")

        if packet.dst_ == self:
            print "packet arrived at dst!"
            packet.message_.on_complete_()
            self.network_._inject_program_messages()
        else:
            link = self.route_[packet.dst_]
            link.send_packet(packet)

    def reset(self):
        pass

class Link(Uuid):
    def __init__(self, network, bandwidth, delay):
        self.bandwidth_ = float(bandwidth)
        self.busy_ = False
        self.delay_ = float(delay)
        self.dst_ = None
        self.network_ = network
        self.queue_ = []
        self.src_ = None
        Uuid.__init__(self)

    def __str__(self):
        return "link" + str(self.uuid_)

    def send(self):
        assert self.dst_
        if self.queue_:
            packet = self.queue_[0]
            self.queue_ = self.queue_[1:]

            packet.link_ = self

            tx_time = packet.size_ * 8 / self.bandwidth_
            self.network_.schedule(EventTxDone(self), tx_time)
            self.network_.schedule(EventRecv(self.dst_, packet), tx_time + self.delay_)
            self.busy_ = True

    def send_packet(self, packet):
        self.queue_ += [packet]
        if not self.busy_:
            self.send()

    def handle(self, event):
        if isinstance(event, EventTxDone):
            self.busy_ = False
            self.send()
        else:
            raise UnhandledEventError

    def reset(self):
        self.busy_ = False
        self.queue = []

class Message(Uuid):
    NO_OP = lambda *args, **kwargs: None
    def __init__(self, src_node, dst_node, count):
        self.src_ = src_node
        self.dst_ = dst_node
        self.count_ = count
        self.on_complete_ = lambda *args, **kwargs: None # Function called when message is delivered
        super(Message, self).__init__()

    def make_packets(self, max_packet_size):
        packets = []
        for i in range(0, self.count_, max_packet_size):
            packet_size = min(max_packet_size, self.count_ - i)
            packets += [Packet(self, self.dst_, None, -1, packet_size)]

        ## Give each packet a sequence number
        for i in range(len(packets)):
            packets[i].sequence_number_ = i

        ## Tell each packet which packet is next
        for i in range(0, len(packets)-1):
            packets[i].next_packet_ = packets[i+1]

        return packets

class Network(object):

    def __init__(self):
        self.time_ = 0.0
        self.events_ = PQ()
        self.graph_ = {}
        self.program_ = Program()

    def __str__(self):
        s = ""
        for src_node, dsts in self.graph_.iteritems():
            for dst_node, link in dsts.iteritems():
                s += str(src_node) + " -> " + str(dst_node) + " == " + str(link) + "\n"
        return s

    def reset(self):
        for src, dsts in self.graph_.iteritems():
            src.reset()
            for dst, link in dsts.iteritems():
                dst.reset()
                link.reset()
        self.time_ = 0.0

    def add_node(self, node):
        self.graph_[node] = {}
        return node

    def join_symmetric(self, n1, n2, bandwidth, delay):
        self.join(n1, n2, Link(self, bandwidth, delay))
        self.join(n2, n1, Link(self, bandwidth, delay))

    def join(self, src_node, dst_node, link):
        link.dst_ = dst_node
        link.src_ = src_node
        self.graph_.setdefault(src_node, {})[dst_node] = link
        src_node.links_[dst_node] = link

    def schedule(self, event, delay):
        self.events_.add_task(event, self.time_ + delay)

    def inject(self, message):
        packet_size = 128
        packets = message.make_packets(packet_size)
        for packet in packets:
            message.src_.forward(packet)
        return message

    def bfs_paths(self, start, goal):
        queue = [(start, [start])]
        while queue:
            (node, path) = queue.pop(0)
            nbrs = set(self.graph_[node].keys())
            for nbr in nbrs - set(path):
                if nbr == goal:
                    yield path + [nbr]
                else:
                    queue.append((nbr, path + [nbr]))


    def initialize_routes(self):
        for src_node in self.graph_:
            src_node.network_ = self
            for dst_node in self.graph_:
                if src_node is not dst_node:
                    paths = [p for p in self.bfs_paths(src_node, dst_node)]
                    shortest_path = paths[0]
                    src_node.route_[dst_node] = src_node.links_[shortest_path[1]]


    # def dump_edge_use(self, init=False):
    #     if init:
    #         csvfile = open("edge-contention.csv", "w")
    #         writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #         row = ["time"] + [i for i in range(len(self.edges))]
    #         writer.writerow(row)
    #     else:
    #         csvfile = open("edge-contention.csv", "a")
    #         writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #     row = [self.time] + [len(edge.active_messages) for edge in self.edges]
    #     writer.writerow(row)
    #     csvfile.close()

    # def trigger_pending(self):
    #     '''Inject any pending messages with no dependencies'''
    #     issued = []
    #     for pending_message, deps in self.pending.iteritems():
    #         if not deps: # not waiting on anything
    #             message = pending_message.message
    #             delay = pending_message.delay
    #             self.events.add_task(InjectMessageEvent(message), self.time + delay)
    #             issued += [pending_message]
    #     for key in issued:
    #         del self.pending[key]
    #     if issued:
    #         print "issued", len(issued), "messages"

    def _inject_program_messages(self):
        for msg in self.program_.ready_messages():
            self.inject(msg)

    def run(self):

        # self.dump_edge_use(init=True)
        self._inject_program_messages()

        while len(self.events_) > 0:
            self.time_, event = self.events_.pop_task()
            print "Simulation @ " + str(self.time_)+"s:", event
            
            event.handler_.handle(event)



        return self.time_

    def run_program(self, program):
        self.program_ = program
        self.run()