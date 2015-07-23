import pdb, traceback, sys
from BBG import startRTSubscriptions
from ADR import getUniverse
from Data import processRealTimeData, Memory

if __name__ == '__main__':
    try:
        memory = Memory()
        universe = getUniverse()
        event_handler = processRealTimeData
        startRTSubscriptions(universe, event_handler, memory)
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
