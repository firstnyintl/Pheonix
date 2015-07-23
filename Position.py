def buildPosition(order):

    # Set VWAP start to timestamp of order
    start = timestamp

    # If until close, figure out close time of exchange
    end = getMarketClose(security, timestamp.date())
