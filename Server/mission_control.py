import zmq
import pandas as pd

from subscriptions import initialize_subscriptions


def subscription_sockets():

    context = zmq.Context()
    sender = context.socket(zmq.PUSH)
    receiver = context.socket(zmq.PULL)
    sender.bind("inproc://subscriptions")
    receiver.connect("inproc://subscriptions")

    return sender, receiver


def initialize_mission_control(memory):

    # Get subscription sockets
    subscription_sender, subscription_receiver = subscription_sockets()

    # Initialize subscriptions
    initialize_subscriptions(subscription_sender)

    # Initialize Feed Handler
    while True:
        message = subscription_receiver.recv()
        ticker = message.split('_', 1)[0]
        message = pd.read_msgpack(message.split('_', 1)[1])
        print message
