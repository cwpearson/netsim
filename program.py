import network

class Program(object):
    def __init__(self):
        self.deps_ = {}

    def add(self, message, after=set()):
        if isinstance(after, network.Message):
            after = set([after])
        message.on_complete_ = lambda: self.notify_delivered(message)
        self.deps_[message] = set(after)
        return message

    def notify_delivered(self, message):
        for msg, waiting_for in self.deps_.iteritems():
            waiting_for.discard(message)

    def pop_ready_messages(self):
        ready = []
        for msg, waiting_for in self.deps_.iteritems():
            if not waiting_for:
                ready += [msg]

        for msg in ready:
            del self.deps_[msg]

        return ready