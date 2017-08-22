import csv
from priorityqueue import PQ 

NO_OP = lambda *args, **kwargs: None

class Publisher(object):
    def __init__(self):
        pass

class MessagePublisher(object):
    def __init__(self):
        self._on_finish = NO_OP

    def on_finish(self, func):
        self._on_finish = func





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
    def __init__(self, bandwidth, latency=0.0):
        self.bandwidth = float(bandwidth)
        self.latency = float(latency)
        self.active_messages = set()

    def reset(self):
        self.active_messages = set()

    def effective_bandwidth(self):
        return self.bandwidth / len(self.active_messages)


class Edge(object):
    def __init__(self, bandwidth, latency=0.0):
        self.bandwidth = float(bandwidth)
        self.latency = float(latency)
        self.active_messages = set()

    def reset(self):
        self.active_messages = set()

    def effective_bandwidth(self):
        return self.bandwidth / len(self.active_messages)


class Network(object):

    def __init__(self):
        self.time = 0.0
        self.events = PQ()
        self.graph = {}
        self.edges = []
        self.nodes = []
        self.messages = {}
        self.pending = {}

    def reset(self):
        for e in self.edges:
            e.reset()
        for n in self.nodes:
            n.reset()
        self.messages = {}
        self.pending = {}
        self.time = 0.0

    def add_node(self, node):
        self.nodes += [node]
        return len(self.nodes) - 1

    def join(self, n1, n2, edge):
        assert n1 < len(self.nodes)
        assert n2 < len(self.nodes)

        # If there is an existing edge, overwrite it
        if n1 in self.graph:
            if n2 in self.graph[n1]:
                edge_id = self.graph[n1][n2]
                self.edges[edge_id] = edge
                return

        self.edges += [edge]
        edge_idx = len(self.edges) - 1
        self.graph.setdefault(n1, {})[n2] = edge_idx
        self.graph.setdefault(n2, {})[n1] = edge_idx


    def inject(self, message, waitfor=[], delay=0.0):
        p = PendingMessage(message, delay)
        self.pending[p] = set(waitfor)
        # self.events.add_task(InjectMessageEvent(message), self.time + delay)
        return message

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


    def get_node_bandwidth(self, route_nodes):
        return min([self.nodes[node_id].effective_bandwidth() for node_id in route_nodes])

    def get_edge_bandwidth(self, route_edges):
        return min([self.edges[edge_id].effective_bandwidth() for edge_id in route_edges])

    def get_node_latency(self, route_nodes):
        return sum([self.nodes[node_id].latency for node_id in route_nodes])

    def get_edge_latency(self, route_edges):
        return sum([self.edges[edge_id].latency for edge_id in route_edges])

    def update_message_finishes(self):
        new_priorities = {}
        for priority, count, event in self.events.pq:
            if isinstance(event, FinishMessageEvent):
                message = self.messages[event.message_id]
                node_bandwidth = self.get_node_bandwidth(message.nodes)
                edge_bandwidth = self.get_edge_bandwidth(message.edges)
                route_bandwidth = min(node_bandwidth, edge_bandwidth)
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

        self.dump_edge_use(init=True)

        self.trigger_pending()

        while len(self.events) > 0:

            self.time, event = self.events.pop_task()
            print "Simulation @ " + str(self.time)+"s:", event

            ## Update message progress since last event
            for edge in self.edges:
                for message_id in edge.active_messages:
                    message = self.messages[message_id]
                    node_bandwidth = self.get_node_bandwidth(message.nodes)
                    edge_bandwidth = self.get_edge_bandwidth(message.edges)
                    route_bandwidth = min(node_bandwidth, edge_bandwidth)
                    message.progress += (self.time - message.last_update_time) * route_bandwidth
                    message.last_update_time = self.time
            
            # for message in self.messages.itervalues():
            #     print "Active message", message.id_, "progress:", message.progress, "/", message.count

            if isinstance(event, TxMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                ## get the route the message will take
                message = self.messages[event.message_id]
                print "TxMessageEvent message:", message


                ## Set the time on the message entering the network
                message.last_update_time = self.time

                ## Update the required time for each active message
                for edge in message.edges:
                    self.edges[edge].active_messages.add(message.id_)
                for node in message.nodes:
                    self.nodes[node].active_messages.add(message.id_)

                ## Update how muh time each message will need to finish
                self.update_message_finishes()

                route_bandwidth = min([self.edges[edge].effective_bandwidth() for edge in message.edges])
                # print "bw:", route_bandwidth

                current_required_time = message.count / route_bandwidth
                # print "message", message.id_, "needs", current_required_time

                # print "processing", message
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
                for node in message.nodes:
                    self.nodes[node].active_messages.remove(message.id_)

                ## update how much time messages need to finish
                self.update_message_finishes()

                ## Trigger what happens when a message is finished
                for deps in self.pending.itervalues():
                    if message in deps:
                        deps.remove(message)

                self.dump_edge_use()


            elif isinstance(event, InjectMessageEvent):
                message = event.message
                self.messages[message.id_] = message

                ## Determine the route the message will take
                message.nodes = list(self.bfs_paths(message.src, message.dst))[0]
                for idx in range(len(message.nodes) - 1):
                    message.edges += [self.graph[message.nodes[idx]][message.nodes[idx+1]]]

                node_latency = self.get_node_latency(message.nodes)
                edge_latency = self.get_edge_latency(message.edges)

                # Model the latency by delating the transmit time
                tx_time = self.time + node_latency + edge_latency
                print "message will be tx @", tx_time, "on route", message.nodes
                self.events.add_task(TxMessageEvent(message.id_), tx_time)

                

            
            else:
                assert False

            self.trigger_pending()

        return self.time