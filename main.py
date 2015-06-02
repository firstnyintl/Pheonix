import pdb, traceback, sys
from Backtest import globalBacktest, backtest, ADR_basic, optimize
from Data import updateTickData

if __name__ == '__main__':
    try:
        # globalBacktest(method='multiprocess', load_data='upfront', core_multiplier=1)
        # backtest(ADR_basic, 'SDRL US Equity')
        optimize('SDRL US Equity', ADR_basic)
        # updateTickData()
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
