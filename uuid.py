class Uuid(object):
    uuid = 0
    def __init__(self):
        self.uuid_ = self.new_uuid()

    def new_uuid(self):
        Uuid.uuid += 1
        return Uuid.uuid - 1