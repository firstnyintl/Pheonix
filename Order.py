from datetime import datetime
import pdb
import pytz
from Data import getExchangeTimesByTicker


class Order:

    def __init__(self, security, size, order_type, timestamp, algo, params):

        self.security = security
        self.size = size
        self.order_type = order_type
        self.date = timestamp
        self.algo = algo
        self.params = params

        exchange_info = getExchangeTimesByTicker(security)

        # Build order
        if self.algo == 'VWAP':

            # Get interval type
            interval = params[0]

            if interval == 'until_close':

                self.start = 'Now'
                self.end = 'MktClose'

            if interval == 'after_open':

                self.start = '

                self.start_time

                pdb.set_trace()
