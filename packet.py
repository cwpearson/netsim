from uuid import Uuid

class Packet(Uuid):
    def __init__(self, dst, next_packet, sequence_number, payload_size):
        self.dst_ = dst # where this packet is trying to go
        self.link_ = None # Current link being traversed
        self.next_packet_ = next_packet # next packet in the message
        self.sequence_number_ = sequence_number
        self.payload_size_ = payload_size # size in bytes
        super(Packet, self).__init__()

    def size(self):
        raise NotImplementedError

class ZeroOverheadPacket(Packet):
    def __init__(self, dst, next_packet, sequence_number, payload_size):
        super(ZeroOverheadPacket, self).__init__(dst, next_packet, sequence_number, payload_size)
    def size(self):
        return self.payload_size_

class Pcie2TLP(Packet):
    def __init__(self, dst, next_packet, sequence_number, header_size, payload_size):
        self.header_size_ = header_size
        super(Pcie2TLP, self).__init__(dst, next_packet, sequence_number, payload_size)

    def size(self):
        # start, sequence, header, payload, ECRC, LCRC, End
        return 1 + 2 + self.header_size_ + self.payload_size_ + 4 + 1

            