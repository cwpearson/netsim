import csv
from priorityqueue import PQ
import itertools
import traceback
import sys


class ProgressUpdate():
    def __init__(self, progress=0.0, last_update=0.0):
        self.progress = progress
        self.last_update = last_update

    def __repr__(self):
        return "(" + str(self.progress) + ", " + str(self.last_update) + ")"


class Message():
    next_id = 0

    def __init__(self, src, dst, count):
        self.id_ = Message.next_id
        Message.next_id += 1
        self.src = src
        self.dst = dst
        self.count = count
        self.nodes = []
        self.edges = []
        self.remaining_nodes = []
        self.remaining_edges = []
        self.edge_progress = {}
        self.node_progress = {}

    def __repr__(self):
        return "[" + str(self.id_) + "] " + str(self.src) + "->" + str(self.dst) + " " + str(self.remaining_nodes)


class PendingMessage():
    def __init__(self, message, delay):
        self.message = message
        self.delay = delay


class Event(object):
    next_id = 0

    def __init__(self,):
        self.id_ = Event.next_id
        Event.next_id += 1


class InjectMessageEvent(Event):
    def __init__(self, message):
        super(InjectMessageEvent, self).__init__()
        self.message = message


class NetworkDeliveredMessageEvent(Event):
    def __init__(self, message_id):
        super(NetworkDeliveredMessageEvent, self).__init__()
        self.message_id = message_id


class NodeRecvMessageEvent(Event):
    def __init__(self, message_id):
        super(NodeRecvMessageEvent, self).__init__()
        self.message_id = message_id


class EdgeRecvMessageEvent(Event):
    def __init__(self, message_id):
        super(EdgeRecvMessageEvent, self).__init__()
        self.message_id = message_id


class NodeFinishMessageEvent(Event):
    def __init__(self, node_id, message_id,):
        super(NodeFinishMessageEvent, self).__init__()
        self.node_id = node_id
        self.message_id = message_id


class EdgeFinishMessageEvent(Event):
    def __init__(self, edge_id, message_id):
        super(EdgeFinishMessageEvent, self).__init__()
        self.edge_id = edge_id
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
        self.messages = {}  # id to message
        self.pending = {}  # pendign to deps

    def reset(self):
        self.time = 0.0
        self.events = PQ()
        for e in self.edges:
            e.reset()
        for n in self.nodes:
            n.reset()
        self.messages = {}
        self.pending = {}

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

    def active_messages(self):
        ret = set()
        for edge in self.edges:
            ret |= edge.active_messages
        for node in self.nodes:
            ret |= node.active_messages
        return ret

    def message_active_nodes(self, message):
        return set([node for node in self.nodes if message in node.active_messages])

    def message_active_edges(self, message):
        return set([edge for edge in self.edges if message in edge.active_messages])

    def get_node_bandwidth(self, route_nodes):
        return min([node.effective_bandwidth() for node in route_nodes])

    def get_edge_bandwidth(self, route_edges):
        return min([edge.effective_bandwidth() for edge in route_edges])

    def get_node_latency(self, route_nodes):
        return sum([node.latency for node in route_nodes])

    def get_edge_latency(self, route_edges):
        return sum([edge.latency for edge in route_edges])

    def current_message_throughput(self, message):
        active_nodes = self.message_active_nodes(message)
        active_edges = self.message_active_edges(message)
        if active_nodes and active_edges:
            return min(self.get_node_bandwidth(active_nodes), self.get_edge_bandwidth(active_edges))
        elif active_nodes:
            return self.get_node_bandwidth(active_nodes)
        elif active_edges:
            return self.get_edge_bandwidth(active_edges)
        else:
            assert False and "message is not active on any nodes or edges?"

    def update_event_priorities(self):
        print "Updating event priority at", self.time

        self.update_message_progress()

        print "Updating event priority after updating progress"

        new_remaining = {}
        for priority, count, event in self.events.pq:
            if event != PQ.REMOVED:
                message = self.messages[event.message_id]
                if isinstance(event, NodeFinishMessageEvent):
                    throughput = self.current_message_throughput(message)
                    node = self.nodes[event.node_id]
                    progress = message.node_progress[node].progress
                    assert progress >= 0
                    time_remaining = (message.count - progress) / throughput
                    print time_remaining, message.count, progress, throughput
                    assert time_remaining >= 0
                    new_remaining[event] = time_remaining
                elif isinstance(event, EdgeFinishMessageEvent):
                    throughput = self.current_message_throughput(message)
                    edge = self.edges[event.edge_id]
                    progress = message.edge_progress[edge].progress
                    assert progress >= 0
                    time_remaining = (message.count - progress) / throughput
                    assert time_remaining >= 0
                    new_remaining[event] = time_remaining

        for event, time_remaining in new_remaining.iteritems():
            self.add_event(event, time_remaining)

    def update_message_progress(self):
        print "Updating all message progress at", self.time
        # traceback.print_stack(file=sys.stdout)
        for message in self.active_messages():
            throughput = self.current_message_throughput(message)
            print "active message:", message, "has throughput:", throughput
            for element, progress in itertools.chain(message.edge_progress.iteritems(), message.node_progress.iteritems()):
                print "progress update on", element, "from", progress, "->",
                if throughput == float('inf'):
                    progress.progress = message.count
                else:
                    progress.progress += (self.time -
                                          progress.last_update) * throughput
                progress.last_update = self.time
                print progress

    def dump_edge_use(self, init=False):
        if init:
            csvfile = open("edge-contention.csv", "w")
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            row = ["time"] + [i for i in range(len(self.edges))]
            writer.writerow(row)
        else:
            csvfile = open("edge-contention.csv", "a")
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        row = [self.time] + [len(edge.active_messages) for edge in self.edges]
        writer.writerow(row)
        csvfile.close()

    def trigger_pending(self):
        '''Inject any pending messages with no dependencies'''
        issued = []
        for pending_message, deps in self.pending.iteritems():
            if not deps:  # not waiting on anything
                message = pending_message.message
                delay = pending_message.delay
                self.add_event(InjectMessageEvent(
                    message), delay)
                issued += [pending_message]
        for key in issued:
            del self.pending[key]
        if issued:
            print "issued", len(issued), "messages"

    def add_event(self, event, delay):
        assert delay != float('inf')
        for priority, count, existing_event in self.events.pq:
            if event == existing_event and priority != self.time + delay:
                print "updating event priority:", priority, self.time + delay
        self.events.add_task(event, self.time + delay)

    def time_field(self):
        return str(self.time) + "::"

    def run(self):

        self.dump_edge_use(init=True)

        self.trigger_pending()

        while len(self.events) > 0:

            self.time, event = self.events.pop_task()
            print "Simulation @ " + str(self.time) + "s:", event

            self.update_message_progress()

            # for message in self.messages.itervalues():
            #     print "Active message", message.id_, "progress:", message.progress, "/", message.count

            if isinstance(event, InjectMessageEvent):
                message = event.message
                self.messages[message.id_] = message

                # Determine the route the message will take
                message.nodes = list(self.bfs_paths(
                    message.src, message.dst))[0]
                for idx in range(len(message.nodes) - 1):
                    message.edges += [self.graph[message.nodes[idx]]
                                      [message.nodes[idx + 1]]]

                # Start the transmit
                print "message will be tx immediately on route", message.nodes

                message.remaining_nodes = list(message.nodes)
                message.remaining_edges = list(message.edges)
                self.add_event(
                    NodeRecvMessageEvent(message.id_), 0)

            elif isinstance(event, NodeRecvMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                node_id = message.remaining_nodes[0]
                message.remaining_nodes = message.remaining_nodes[1:]

                # The node is now participating in the message
                node = self.nodes[node_id]
                node.active_messages.add(message)

                # Just getting started on this node
                message.node_progress[node] = ProgressUpdate(0, self.time)

                # If there are further nodes along the route, forward the message
                if message.remaining_edges:
                    # Edge will recv after processing time
                    self.add_event(EdgeRecvMessageEvent(
                        message.id_), node.latency)

                    # Schedule this node to be done with the message based on the current message throughput
                    progress = message.node_progress[node]
                    delay = (message.count - progress.progress) / \
                        self.current_message_throughput(message)
                    assert delay >= 0
                    self.add_event(NodeFinishMessageEvent(
                        node_id, message.id_), delay)

                # This activity may affect other messages
                self.update_event_priorities()

            elif isinstance(event, EdgeRecvMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                edge_id = message.remaining_edges[0]
                message.remaining_edges = message.remaining_edges[1:]

                edge = self.edges[edge_id]
                edge.active_messages.add(message)

                # Just getting started on this edge
                message.edge_progress[edge] = ProgressUpdate(0, self.time)

                # Node recvs after our latency
                self.add_event(NodeRecvMessageEvent(
                    message.id_), self.edges[edge_id].latency)

                # We finish based on the message throughput
                progress = message.edge_progress[edge]
                delay = (message.count - progress.progress) / \
                    self.current_message_throughput(message)
                assert delay >= 0
                self.add_event(EdgeFinishMessageEvent(
                    edge_id, message.id_), delay)

                # This activity may affect other messages
                self.update_event_priorities()

                self.dump_edge_use()

            elif isinstance(event, NodeFinishMessageEvent):
                message = self.messages[event.message_id]
                node = self.nodes[event.node_id]

                # Remove the message from active messages
                node.active_messages.remove(message)

                # Remove the progress record from the message
                assert node in message.node_progress
                del message.node_progress[node]

                # If this is the last node in the message, notify that network has finished
                if event.node_id == message.nodes[-1]:
                    print "last node in message, adding deliveredmessagevent"
                    self.add_event(
                        NetworkDeliveredMessageEvent(event.message_id), 0)

                # This completion may affect other messages
                print "Updating priorities after node finished"
                self.update_event_priorities()

            elif isinstance(event, EdgeFinishMessageEvent):
                message = self.messages[event.message_id]
                node = self.nodes[event.edge_id]

                # Remove the message from active messages
                self.edges[event.edge_id].active_messages.remove(message)

                # Remove the progress record from the message
                assert edge in message.edge_progress
                del message.edge_progress[edge]

                # This completion may affect other messages
                self.update_event_priorities()

            elif isinstance(event, NetworkDeliveredMessageEvent):
                # Print the state of the links before we stop transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                del self.messages[event.message_id]

                print self.time, "Message", message, "completed!"

                # update how much time messages need to finish

                # Trigger what happens when a message is finished
                for deps in self.pending.itervalues():
                    if message in deps:
                        deps.remove(message)

                self.dump_edge_use()

            else:
                assert False

            self.trigger_pending()

        return self.time
