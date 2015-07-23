import blpapi
import numpy as np
import pandas as pd
from BBG import createSession, endSession
import pdb


def getTeams():
    """
    Returns numpy array with team names
    """
    session = createSession()
    session.openService("//blp/emapisvc")
    EMSXservice = session.getService("//blp/emapisvc")
    request = EMSXservice.createRequest("GetTeams")
    requestID = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=requestID)

    # Start loop
    try:
        while(True):
            ey = session.nextEvent()
            for msg in ey:
                if ey.eventType() == blpapi.Event.RESPONSE:
                    if msg.correlationIds()[0].value() == requestID.value():

                        # If error, print error message
                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print "ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage)

                        # Otherwise get teams 
                        elif msg.messageType() == blpapi.Name("GetTeams"):
                            teams = msg.getElement("TEAMS")

                            # Convert to array and return
                            teams = np.asarray([x for x in teams.values()])
                            return teams
    finally:
        endSession(session)


def getBrokers(ticker):
    """
    Returns numpy array with broker names
    """
    session = createSession()
    session.openService("//blp/emapisvc")
    EMSXservice = session.getService("//blp/emapisvc")
    request = EMSXservice.createRequest("GetBrokers")

    # Set ticker
    request.set("EMSX_TICKER", ticker)

    requestID = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=requestID)

    # Start loop
    try:
        while(True):
            ey = session.nextEvent()
            for msg in ey:
                if ey.eventType() == blpapi.Event.RESPONSE:
                    if msg.correlationIds()[0].value() == requestID.value():

                        # If error, print error message
                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print "ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage)

                        # Otherwise get teams 
                        elif msg.messageType() == blpapi.Name("GetBrokers"):
                            brokers = msg.getElement("EMSX_BROKERS")

                            # Convert to array and return
                            brokers = np.asarray([x for x in brokers.values()])
                            return brokers
    finally:
        endSession(session)


def getBrokerStrategies(ticker, broker):
    """
    Returns numpy array with strategies for a specific broker
    """
    session = createSession()
    session.openService("//blp/emapisvc")
    EMSXservice = session.getService("//blp/emapisvc")
    request = EMSXservice.createRequest("GetBrokerStrategies")

    # Set ticker
    request.set("EMSX_TICKER", ticker)
    request.set("EMSX_BROKER", broker)

    requestID = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=requestID)

    # Start loop
    try:
        while(True):
            ey = session.nextEvent()
            for msg in ey:
                if ey.eventType() == blpapi.Event.RESPONSE:
                    if msg.correlationIds()[0].value() == requestID.value():

                        # If error, print error message
                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print "ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage)

                        # Otherwise get teams 
                        elif msg.messageType() == blpapi.Name("GetBrokerStrategies"):
                            strategies = msg.getElement("EMSX_STRATEGIES")

                            # Convert to array and return
                            strategies = np.asarray([x for x in strategies.values()])
                            return strategies
    finally:
        endSession(session)


def getBrokerStrategyParams(ticker, broker, strategy):
    """
    Returns numpy array with fields for a specific strategy
    """
    session = createSession()
    session.openService("//blp/emapisvc")
    EMSXservice = session.getService("//blp/emapisvc")
    request = EMSXservice.createRequest("GetBrokerStrategyInfo")

    # Set ticker
    request.set("EMSX_TICKER", ticker)
    request.set("EMSX_BROKER", broker)
    request.set("EMSX_STRATEGY", strategy)

    requestID = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=requestID)

    # Start loop
    try:
        while(True):
            ey = session.nextEvent()
            for msg in ey:
                if ey.eventType() == blpapi.Event.RESPONSE:
                    if msg.correlationIds()[0].value() == requestID.value():

                        # If error, print error message
                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print "ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage)

                        # Otherwise get teams 
                        elif msg.messageType() == blpapi.Name("GetBrokerStrategyInfo"):
                            strategies = msg.getElement("EMSX_STRATEGY_INFO")

                            # Convert to array and return
                            params = np.asarray([x.getElementAsString("FieldName") for x in strategies.values()])
                            return params
    finally:
        endSession(session)


def getOrderInfo(order_id, aggregate=True):
    """
    Returns dataframe with information for a specific order
    """
    session = createSession()
    session.openService("//blp/emapisvc")
    EMSXservice = session.getService("//blp/emapisvc")
    request = EMSXservice.createRequest("OrderInfo")

    # Set ticker
    # request.set("EMSX_SEQUENCE", order_id)
    request.set("EMSX_SEQUENCE", order_id)
    request.set("EMSX_IS_AGGREGATED", int(aggregate))

    requestID = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=requestID)

    # Start loop
    try:
        while(True):
            ey = session.nextEvent()
            for msg in ey:
                if ey.eventType() == blpapi.Event.RESPONSE:
                    if msg.correlationIds()[0].value() == requestID.value():

                        # If error, print error message
                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print "ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage)

                        # Otherwise get teams 
                        elif msg.messageType() == blpapi.Name("OrderInfo"):

                            order = {}
                            order['amount'] = msg.getElementAsInteger("EMSX_AMOUNT")
                            order['avg_price'] = msg.getElementAsFloat("EMSX_AVG_PRICE")
                            order['basket_name'] = msg.getElementAsString("EMSX_BASKET_NAME")
                            order['broker'] = msg.getElementAsString("EMSX_BROKER")
                            order['exchange'] = msg.getElementAsString("EMSX_EXCHANGE")
                            order['filled'] = msg.getElementAsInteger("EMSX_FILLED")
                            order['flag'] = msg.getElementAsInteger("EMSX_FLAG")
                            order['idle_amount'] = msg.getElementAsInteger("EMSX_IDLE_AMOUNT")
                            order['limit_price'] = msg.getElementAsFloat("EMSX_LIMIT_PRICE")
                            order['notes'] = msg.getElementAsString("EMSX_NOTES")
                            order['order_create_date'] = msg.getElementAsString("EMSX_ORDER_CREATE_DATE")
                            order['order_create_time'] = msg.getElementAsString("EMSX_ORDER_CREATE_TIME")
                            order['order_type'] = msg.getElementAsString("EMSX_ORDER_TYPE").replace(' ', '')
                            order['port_mgr'] = msg.getElementAsString("EMSX_PORT_MGR").replace(' ', '')
                            order['position'] = msg.getElementAsString("EMSX_POSITION")
                            order['side'] = msg.getElementAsString("EMSX_SIDE")
                            order['step_out_broker'] = msg.getElementAsString("EMSX_STEP_OUT_BROKER")
                            order['sub_flag'] = msg.getElementAsInteger("EMSX_SUB_FLAG")
                            order['ticker'] = msg.getElementAsString("EMSX_TICKER")
                            order['tif'] = msg.getElementAsString("EMSX_TIF")
                            order['trader'] = msg.getElementAsString("EMSX_TRADER").replace(' ', '')
                            order['trader_uuid'] = msg.getElementAsInteger("EMSX_TRADER_UUID")
                            order['ts_ordnum'] = msg.getElementAsInteger("EMSX_TS_ORDNUM")
                            order['working'] = msg.getElementAsInteger("EMSX_WORKING")
                            order['yellow_key'] = msg.getElementAsString("EMSX_YELLOW_KEY")

                            order = pd.Series(order)

                            return order
    finally:
        endSession(session)
