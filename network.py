import csv
from priorityqueue import PQ
import itertools
import traceback
import sys
import pprint

pp = pprint.PrettyPrinter(depth=2)


class ActivityRecord(object):
    def __init__(self, value, last_update):
        self.value = float(value)
        self.last_update = last_update

    def __str__(self):
        return "ActivityRecord" + repr(vars(self))


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

    def __str__(self):
        # return "[" + repr(self.id_) + "] " + repr(self.src) + "->" + repr(self.dst)
        return self.__class__.__name__ + repr(vars(self))

    def get_next_node_id(self, edge_id):
        return self.nodes[self.edges.index(edge_id) + 1]

    def get_next_edge_id(self, node_id):
        return self.edges[self.nodes.index(node_id)]


class PendingMessage():
    def __init__(self, message, delay):
        self.message = message
        self.delay = delay


class Event(object):
    next_id = 0

    def __init__(self,):
        self.id_ = Event.next_id
        Event.next_id += 1

    def __str__(self):
        return self.__class__.__name__ + repr(vars(self))


class InjectMessageEvent(Event):
    def __init__(self, message):
        super(InjectMessageEvent, self).__init__()
        self.message = message


class NetworkDeliveredMessageEvent(Event):
    def __init__(self, message_id):
        super(NetworkDeliveredMessageEvent, self).__init__()
        self.message_id = message_id


class NodeRecvMessageEvent(Event):
    def __init__(self, message_id, node_id):
        super(NodeRecvMessageEvent, self).__init__()
        self.message_id = message_id
        self.node_id = node_id


class EdgeRecvMessageEvent(Event):
    def __init__(self, message_id, edge_id):
        super(EdgeRecvMessageEvent, self).__init__()
        self.message_id = message_id
        self.edge_id = edge_id


class NodeFinishMessageEvent(Event):
    def __init__(self, node_id, message_id):
        super(NodeFinishMessageEvent, self).__init__()
        self.message_id = message_id
        self.node_id = node_id


class EdgeFinishMessageEvent(Event):
    def __init__(self, edge_id, message_id):
        super(EdgeFinishMessageEvent, self).__init__()
        self.message_id = message_id
        self.edge_id = edge_id


class GraphElement(object):
    def __init__(self, bandwidth, latency=0.0):
        self.bandwidth = float(bandwidth)
        self.latency = float(latency)
        self.reset()

    def reset(self):
        self.active_messages = {}

    def effective_bandwidth(self):
        return self.bandwidth / len(self.active_messages)

    def __str__(self):
        return self.__class__.__name__ + repr(vars(self))


class Node(GraphElement):
    def __init__(self, bandwidth, latency=0.0):
        super(Node, self).__init__(bandwidth, latency)


class Edge(GraphElement):
    def __init__(self, bandwidth, latency=0.0):
        super(Edge, self).__init__(bandwidth, latency)


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
            ret |= set(edge.active_messages.keys())
        for node in self.nodes:
            ret |= set(node.active_messages.keys())
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
            # assert False and "message is not active on any nodes or edges?"
            return 0

    def message_time_remaining(self, message, graph_element):
        print "time remaining for", message, "on", graph_element
        activity = graph_element.active_messages[message]
        progress = activity.value
        delay = (message.count - progress) / \
            self.current_message_throughput(message)
        assert delay >= 0
        return delay

    def update_event_priorities(self):
        self.update_active_message_progress()

        print "Updating event priority after updating progress"

        new_remaining = {}
        for priority, count, event in self.events.pq:
            if event != PQ.REMOVED:
                message = self.messages[event.message_id]
                if isinstance(event, NodeFinishMessageEvent):
                    throughput = self.current_message_throughput(message)
                    if throughput == 0:
                        time_remaining = float('inf')
                    else:
                        element = self.nodes[event.node_id]
                        progress = element.active_messages[message].value
                        assert progress >= 0
                        time_remaining = (
                            message.count - progress) / throughput
                    assert time_remaining >= 0
                    new_remaining[event] = time_remaining
                elif isinstance(event, EdgeFinishMessageEvent):
                    throughput = self.current_message_throughput(message)
                    if throughput == 0:
                        time_remaining = float('inf')
                    else:
                        element = self.edges[event.edge_id]
                        progress = element.active_messages[message].value
                        assert progress >= 0
                        time_remaining = (
                            message.count - progress) / throughput
                    assert time_remaining >= 0
                    new_remaining[event] = time_remaining

        for event, time_remaining in new_remaining.iteritems():
            print "Updating", event, "to", time_remaining
            self.add_event(event, time_remaining)

    def update_active_message_progress(self):
        print "Updating all message progress at", self.time
        # traceback.print_stack(file=sys.stdout)
        for element in itertools.chain(self.nodes, self.edges):
            for message, activity in element.active_messages.iteritems():
                throughput = self.current_message_throughput(message)
                print "active message:", message, "on", element, "has throughput", throughput

                print "progress update on", element, "from", activity, "->",
                if throughput == float('inf'):
                    activity.value = message.count
                else:
                    activity.value += (self.time -
                                       activity.last_update) * throughput
                activity.last_update = self.time
                print activity

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
        # for priority, count, existing_event in self.events.pq:
        #     if event == existing_event and priority != self.time + delay:
        #         print "updating event priority:", priority, self.time + delay
        self.events.add_task(event, self.time + delay)

    def time_field(self):
        return "SIM@" + str(self.time) + "::"

    def run(self):

        self.dump_edge_use(init=True)

        self.trigger_pending()

        while self.events:

            self.time, event = self.events.pop_task()
            print self.time_field(), event

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

                self.add_event(
                    NodeRecvMessageEvent(message.id_, message.nodes[0]), 0)

            elif isinstance(event, NodeRecvMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                node_id = event.node_id
                node = self.nodes[node_id]

                # The node is now participating in the message
                node.active_messages[message] = ActivityRecord(0, self.time)

                # If this is the first node in the message, it will finish based on the current message throughput
                if node_id == message.nodes[0]:
                    delay = self.message_time_remaining(message, node)
                    self.add_event(NodeFinishMessageEvent(
                        node_id, message.id_), delay)

                # If there are further nodes along the route, forward the message
                if node_id != message.nodes[-1]:
                    # Edge will recv after processing time
                    print "starting next edge after", node.latency
                    self.add_event(EdgeRecvMessageEvent(
                        message.id_, message.get_next_edge_id(node_id)), node.latency)

                # This activity may affect other messages
                print "update_event_priorities after", event
                self.update_event_priorities()

            elif isinstance(event, NodeFinishMessageEvent):
                message = self.messages[event.message_id]
                node = self.nodes[event.node_id]

                # Remove the message from active messages
                del node.active_messages[message]

                # If this is the last node in the message, notify that network has finished
                print "nodefinish", message.nodes, event.node_id
                if event.node_id == message.nodes[-1]:
                    # if not self.message_active_edges(message) and not self.message_active_nodes(message):
                    print "Last node finished"
                    self.add_event(
                        NetworkDeliveredMessageEvent(event.message_id), 0)
                else:  # otherwise, release the next edge
                    next_edge_id = message.get_next_edge_id(event.node_id)
                    print "releasing next edge:", next_edge_id
                    # delay = self.message_edge_time_remaining(
                    #     message, next_edge)
                    delay = 0
                    self.add_event(EdgeFinishMessageEvent(
                        next_edge_id, event.message_id), delay)

                # This completion may affect other messages
                print "update_event_priorities after", event
                self.update_event_priorities()

            elif isinstance(event, EdgeRecvMessageEvent):
                # Print the state of the links before we start transmitting
                self.dump_edge_use()

                message = self.messages[event.message_id]
                edge_id = event.edge_id
                edge = self.edges[edge_id]

                # Edge is now participating in the message
                edge.active_messages[message] = ActivityRecord(0, self.time)

                # Next node recvs after our latency
                self.add_event(NodeRecvMessageEvent(
                    message.id_, message.get_next_node_id(edge_id)), edge.latency)

                print "update_event_priorities after", event
                self.update_event_priorities()

                self.dump_edge_use()

            elif isinstance(event, EdgeFinishMessageEvent):
                message = self.messages[event.message_id]
                edge = self.edges[event.edge_id]
                print message

                # Next node may finish
                next_node_id = message.get_next_node_id(event.edge_id)
                print "next node may finish:", next_node_id
                next_node = self.nodes[next_node_id]
                delay = self.message_time_remaining(message, next_node)
                self.add_event(NodeFinishMessageEvent(
                    next_node_id, event.message_id), delay)

                # Remove the message from active messages
                del edge.active_messages[message]

                print "update_event_priorities after", event
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
