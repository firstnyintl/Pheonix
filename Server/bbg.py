import blpapi
import numpy as np
import pandas as pd
from optparse import OptionParser
from datetime import datetime
import pytz
import pdb


def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    # parser.add_option("--me",
    #                   dest="maxEvents",
    #                   type="int",
    #                   help="stop after this many events (default: %default)",
    #                   metavar="maxEvents",
    #                   default=1000000)

    (options, args) = parser.parse_args()

    return options


def create_session():
    """
    Creates and returns session object
    """
    options = parseCmdLine()
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print "Failed to start session."
        return
    return session


def process_subscription_message(msg, socket):

    print 'message'

    # Get Ticker
    try:
        ticker = msg.correlationIds()[0].value()
    except:
        raise Exception('Correlation ID Access Error')

    # Process Trades only
    if msg.getElement('MKTDATA_EVENT_TYPE').getValue().__str__() in ['TRADE', 'AT_TRADE']:

        # Process Date
        date = datetime.today().date()
        time = msg.getElement('EVT_TRADE_TIME_RT').getValue()
        dt = datetime.combine(date, time)
        timestamp = pytz.timezone('America/New_York').localize(dt)

        # Get Price
        price = msg.getElement('EVT_TRADE_PRICE_RT').getValue()
        dict = {'Price': price}

        # Get Size and Codes
        if ticker.split(' ')[-1] in ['Equity', 'Index']:
            size = msg.getElement('EVT_TRADE_SIZE_RT').getValue()
            dict['Size'] = size

            if msg.hasElement('EVT_TRADE_CONDITION_CODE_RT'):
                if not msg.getElement('EVT_TRADE_CONDITION_CODE_RT').isNull():
                    codes = msg.getElement('EVT_TRADE_CONDITION_CODE_RT').getValue()
                else:
                    codes = ''
            else:
                    codes = ''

            dict['Codes'] = codes

        # Combine into DataFrame
        df = pd.DataFrame(dict, index=[timestamp])

        # Serialize
        message = df.to_msgpack()

        # Add Ticker
        message = ticker + '_' + message

        print 'Processed ticker'

        # Send via socket
        socket.send(message)
