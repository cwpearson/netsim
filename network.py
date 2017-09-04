import csv
from priorityqueue import PQ 

NO_OP = lambda *args, **kwargs: None

class Event(object):
    def __init__(self, handler):
        self.handler_ = handler

class EventTxDone(Event):
    def __init__(self, link):
        super(EventTxDone, self).__init__(link)

class EventRecv(Event):
    def __init__(self, node):
        super(EventRecv, self).__init__(node)

class Packet(object):
    uid = 0
    def __init__(self):
        self.link_ = None # Current link being traversed
        self.next_packet_ = None # next packet in the message
        self.size_ = None # size in bytes
        self.sequence_number_ = None
        self.uid_ = Packet.uid # unique id for the packet
        Packet.uid += 1

class Handler(object):
    def __init__(self):
        pass

    def handle(self, event):
        raise NotImplementedError

class Node(Handler):
    def __init__(self):
        self.queue = []
        self.links = [] # outgoing links

    def handle(self, event):
        raise NotImplementedError

class Link(object):
    def __init__(self):
        self.bandwidth_ = float('inf')
        self.busy_ = False
        self.delay_ = 0.0
        self.dst_ = None
        self.neighbor_ = None
        self.network_ = None
        self.queue_ = []
        self.src_ = None

    def send(self):
        if self.queue_:
            packet = self.queue_[0]
            self.queue_ = self.queue_[1:]

            packet.src_ = self.src_
            packet.dst_ = self.dst_
            packet.link_ = self

            tx_time = packet.size_ * 8 / self.bandwidth_
            self.network_.schedule(EventTxDone(self), tx_time)
            self.network_.schedule(EventRecv(self.dst_), tx_time + self.delay_)
            self.busy_ = True

class Message():
    next_id = 0

    def __init__(self, src, dst, count):
        self.id_ = Message.next_id
        Message.next_id += 1
        self.src = src
        self.dst = dst
        self.count = count
        self.progress = 0.0
        self.edges = []
        self.nodes = []
        self.last_update_time = 0.0

    def __repr__(self):
        return "["+str(self.id_)+"] " + str(self.src) + " --" + str(int(self.progress))+"/"+str(self.count) + "--> " + str(self.dst)

class PendingMessage():
    def __init__(self, message, delay):
        self.message = message
        self.delay = delay

class InjectMessageEvent(Event):
    def __init__(self, message):
        super(InjectMessageEvent, self).__init__()
        self.message = message

class Network(object):

    def __init__(self):
        self.time_ = 0.0
        self.events_ = PQ()
        self.graph_ = {}

    def reset(self):
        for src, dsts in self.graph_.iteritems():
            src.reset()
            for dst, link in dsts.iteritems():
                dst.reset()
                link.reset()

        self.pending_ = {}
        self.time_ = 0.0

    def add_node(self, node):
        self.graph_[node] = {}
        return node

    def join(self, n1, n2, edge):
        self.graph_.setdefault(n1, {})[n2] = edge
        self.graph_.setdefault(n2, {})[n1] = edge

    def schedule(self, event, delay):
        self.events_.add_task(event, self.time_ + delay)

    def inject(self, message, waitfor=[], delay=0.0):
        p = PendingMessage(message, delay)
        # self.events.add_task(InjectMessageEvent(message), self.time + delay)
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

    def trigger_pending(self):
        '''Inject any pending messages with no dependencies'''
        issued = []
        for pending_message, deps in self.pending.iteritems():
            if not deps: # not waiting on anything
                message = pending_message.message
                delay = pending_message.delay
                self.events.add_task(InjectMessageEvent(message), self.time + delay)
                issued += [pending_message]
        for key in issued:
            del self.pending[key]
        if issued:
            print "issued", len(issued), "messages"

    def run(self):

        # self.dump_edge_use(init=True)

        while len(self.events_) > 0:

            self.time, event = self.events_.pop_task()
            print "Simulation @ " + str(self.time_)+"s:", event
            
            event.handler_.handle(event)

        return self.time