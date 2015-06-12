import pdb, traceback, sys
from Backtest import globalBacktest, backtest, optimize, globalOptimization
from Strategy import ADR_basic
from BBG import updateHistoricalTickData
from Data import updateTickData

if __name__ == '__main__':
    try:
        # globalBacktest(method='multiprocess', load_data='upfront', core_multiplier=1)
        backtest(ADR_basic('SDRL US Equity'))
        # updateHistoricalTickData('SDRL US Equity')
        # updateTickData()
        # globalOptimization()
        # optimize('SDRL US Equity')

    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
