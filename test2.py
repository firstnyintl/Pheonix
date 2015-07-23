import sys
import zmq
import pdb
import time

context = zmq.Context()
sock = context.socket(zmq.PULL)
# sock.connect("tcp://localhost:8080")
sock.connect("inproc://bla")

while True:
    msg = sock.recv()
    print msg
