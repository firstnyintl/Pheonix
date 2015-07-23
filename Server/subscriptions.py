from threading import Thread
import zmq
import blpapi
from ADR import adr_universe #############
from bbg import create_session, process_subscription_message
import pdb


def initial_subscription_tickers():
    """
    Ticker list for initial subscriptions
    """
    return adr_universe()


def start_bloomberg_subscriptions(universe):
    """
    Start Bloomberg Desktop API subscriptions
    """
    # Create session
    session = create_session()

    # Get subscription list
    subscriptions = blpapi.SubscriptionList()

    # Interval (0 = continuous)
    interval = 0

    # Add tickers to subscription
    for ticker in universe:
        subscriptions.add(ticker, "LAST_PRICE", "interval=%s"%(interval), blpapi.CorrelationId(ticker))

    # Start subscriptions
    session.subscribe(subscriptions)

    return session


def start_subscription_channel(session, socket):

    # Plug into feed
    try:
        while True:
            event = session.nextEvent(1000)
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                for msg in event:

                    # Get message and process
                    process_subscription_message(msg, socket)
    finally:
        pass


def initialize_subscriptions(socket):
    """
    Start up data subscriptions
    """
    # Get all tickers needed to subscribe to data for
    universe = initial_subscription_tickers()

    # Start Bloomberg subscriptions
    session = start_bloomberg_subscriptions(universe)

    # Start Subscription channel
    Thread(target=start_subscription_channel, args=(session, socket)).start()
