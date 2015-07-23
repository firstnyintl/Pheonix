from datetime import datetime, timedelta
from Data import getMarketOpen, getMarketClose
import pdb
import pytz


def buildOrder(security, size, timestamp, order_type, algo, params, broker='Tradebook'):
    """
    Build Order according to BBG Tradebook parameters
    """
    # For now assume Tradebook
    if broker == 'Tradebook':

        if algo == 'VWAP':

            if params[0] == 'until_close':
                start = 'Now'
                end = 'MktClose'

            if params[0] == 'after_open':
                start = 'MktOpen'
                end = '+'
                pdb.set_trace()
