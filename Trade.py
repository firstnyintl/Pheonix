

class Trade:

    def __init__(self, security, size, order_type, timestamp, algo, params):

        self.security = security
        self.size = size
        self.oder_type = order_type
        self.timestamp = timestamp
        self.algo = algo
        self.params = params
