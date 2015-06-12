

class Trade:

    def __init__(self, security, size, order_type, timestamp, day_offset, algo, params):

        self.security = security
        self.size = size
        self.oder_type = order_type
        self.date = timestamp
        self.algo = algo
        self.day_offset = day_offset
        self.params = params
