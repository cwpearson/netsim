import heapq as hq
import itertools
import csv
import copy


class PQ(object):
    REMOVED = '<removed entry>'

    def __init__(self):
        self.pq = [] # list of entries arranged in a heap
        self.entry_finder = {} # mapping of tasks to entries
        self.counter = itertools.count()     # unique sequence count                             

    def __len__(self):
        return len(self.pq)

    def add_task(self, task, priority=0):
        'Add a new task or update the priority of an existing task'
        if task in self.entry_finder:
            self.remove_task(task)
        count = next(self.counter)
        entry = [priority, count, task]
        self.entry_finder[task] = entry
        hq.heappush(self.pq, entry)

    def remove_task(self, task):
        'Mark an existing task as REMOVED.  Raise KeyError if not found.'
        entry = self.entry_finder.pop(task)
        entry[-1] = PQ.REMOVED

    def pop_task(self):
        'Remove and return the lowest priority task. Raise KeyError if empty.'
        while self.pq:
            priority, count, task = hq.heappop(self.pq)
            if task is not PQ.REMOVED:
                del self.entry_finder[task]
                return priority, task
        raise KeyError('pop from an empty priority queue')


class Handle(object):
    def __init__(self, network, message=None):
        self.network = network
        self.to_inject = message

    def inject(self, message):
        self.to_inject = message
        self.to_inject.network = self.network
        return self.to_inject.finish_handle

    def __call__(self):
        if self.to_inject:
            return self.network.inject(self.to_inject)


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
        self.last_update_time = 0.0
        self.finish_handle = Handle(None)

    def __repr__(self):
        return "["+str(self.id_)+"] " + str(self.src) + " --" + str(int(self.progress))+"/"+str(self.count) + "--> " + str(self.dst)

class Event(object):
    next_id = 0
    def __init__(self, callbacks=[]):
        self.id_ = Event.next_id
        Event.next_id += 1
        self.callbacks = callbacks

class InjectMessageEvent(Event):
    def __init__(self, message):
        super(InjectMessageEvent, self).__init__()
        self.message = message

class TxMessageEvent(Event):
    def __init__(self, message_id):
        super(TxMessageEvent, self).__init__()
        self.message_id = message_id

class FinishMessageEvent(Event):
    def __init__(self, message_id):
        super(FinishMessageEvent, self).__init__()
        self.message_id = message_id

class Node(object):
    def __init__(self, latency):
        self.latency = latency

class Edge(object):
    def __init__(self, bandwidth):
        self.bandwidth = float(bandwidth)
        self.active_messages = set()

    def effective_bandwidth(self):
        return self.bandwidth / len(self.active_messages)

class Network(object):


    def __init__(self):
        self.time = 0.0
        self.events = PQ()
        self.graph = {}
        self.edges = []
        self.messages = {}

    def join(self, n1, n2, edge):
        self.edges += [edge]
        edge_idx = len(self.edges) - 1
        self.graph.setdefault(n1, {})[n2] = edge_idx
        self.graph.setdefault(n2, {})[n1] = edge_idx


    def inject(self, message):
        message.finish_handle.network = self
        self.events.add_task(InjectMessageEvent(message), self.time)
        return message.finish_handle

    def bfs_paths(self, start, goal):
        queue = [(start, [start])]
        while queue:
            (node, path) = queue.pop(0)
            nbrs = set(self.graph[node].keys())
            for nbr in nbrs - set(path):
                if nbr == goal:
                    yield path + [nbr]
                else:
                    queue.append((nbr, path + [nbr]))


    def get_bandwidth(self, route_edges):
        return min([self.edges[edge_id].effective_bandwidth() for edge_id in route_edges])


    def update_message_finishes(self):
        new_priorities = {}
        for priority, count, event in self.events.pq:
            if isinstance(event, FinishMessageEvent):
                message = self.messages[event.message_id]
                route_bandwidth = self.get_bandwidth(message.edges)
                time_remaining = (message.count - message.progress) / route_bandwidth
                # Update the event with the time remaining
                new_priority = self.time + time_remaining
                assert new_priority >= self.time
                if new_priority != priority:
                    new_priorities[event] = self.time + time_remaining
        for event in new_priorities:
            self.events.add_task(event, new_priorities[event])
            message.progress += (self.time - message.last_update_time) * route_bandwidth


    def dump_edge_use(self, init=False):
        if init:
            csvfile = open("edge-contention.csv", "w")
            writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            row = ["time"] + [i for i in range(len(self.edges))]
            writer.writerow(row)
        else:
            csvfile = open("edge-contention.csv", "a")
            writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        row = [self.time] + [len(edge.active_messages) for edge in self.edges]
        writer.writerow(row)
        csvfile.close()

    def run(self):

        self.dump_edge_use(init=True)

        while len(self.events) > 0:
            self.time, event = self.events.pop_task()
            print "Simulation @ " + str(self.time)+"s:", event

            ## Update message progress since last event
            for edge in self.edges:
                for message_id in edge.active_messages:
                    message = self.messages[message_id]
                    route_bandwidth = self.get_bandwidth(message.edges)
                    message.progress += (self.time - message.last_update_time) * route_bandwidth
                    message.last_update_time = self.time
            
            for message in self.messages.itervalues():
                print "Active message", message.id_, "progress:", message.progress, "/", message.count

            if isinstance(event, TxMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                ## get the route the message will take
                message = self.messages[event.message_id]
                print "TxMessageEvent message:", message


                ## Update the required time for each active message
                for edge in message.edges:
                    self.edges[edge].active_messages.add(message.id_)

                ## Update how muh time each message will need to finish
                self.update_message_finishes()

                route_bandwidth = min([self.edges[edge].effective_bandwidth() for edge in message.edges])
                # print "bw:", route_bandwidth

                current_required_time = message.count / route_bandwidth
                print "message", message.id_, "needs", current_required_time

                print "processing", message
                finish_event = FinishMessageEvent(message.id_)
                message.finish_event = finish_event
                self.events.add_task(finish_event, self.time + current_required_time)


                self.dump_edge_use()

            elif isinstance(event, FinishMessageEvent):
                # Print the state of the links before we stop transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                del self.messages[event.message_id]

                print self.time, "Message", message, "completed!"

                ## remove the message from any links
                for edge in message.edges:
                    self.edges[edge].active_messages.remove(message.id_)

                ## update how much time messages need to finish
                self.update_message_finishes()

                message.finish_handle()
                self.dump_edge_use()


            elif isinstance(event, InjectMessageEvent):
                message = event.message
                self.messages[message.id_] = message

                ## Determine the route the message will take
                route_nodes = list(self.bfs_paths(message.src, message.dst))[0]
                for idx in range(len(route_nodes) - 1):
                    message.edges += [self.graph[route_nodes[idx]][route_nodes[idx+1]]]

                tx_time = self.time
                print "message will be tx @", tx_time, "on route", route_nodes
                self.events.add_task(TxMessageEvent(message.id_), tx_time)

                

            
            else:
                assert False

        print "Simulation @", self.time, "FINISHED!"


n = Network()

# Fat tree
n.join(0, 1, Edge(2**21))
n.join(0, 2, Edge(2**21))
n.join(1, 3, Edge(2**20))
n.join(1, 4, Edge(2**20))
n.join(2, 5, Edge(2**19))
n.join(2, 6, Edge(2**19))


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

# n.after(h1, 0.1, Message(3 ,4, 1024))

n.run()
