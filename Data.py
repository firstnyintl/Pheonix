import msgpack
import pytz
from datetime import timedelta, datetime, time
from multiprocessing import Pool, cpu_count
from functools import partial
from threading import Thread
import pandas as pd
from pandas import HDFStore, Timestamp, DatetimeIndex
from ADR import getORD, getADRFX, getFutures, getADRRatio, calcADRPremium
import numpy as np
import zmq
import BBG
import pdb


class intradayTickMemory():
    memdict = {}


def getNonSettlementDates(ticker, start, stop):
    """
    Get non settlement dates for ticker (1950-2050). List of datetime.date objects
    """
    # Get CDR code
    with open('CDR_codes', 'r') as myfile:
        code = msgpack.unpackb(myfile.read())[ticker.split(' ')[1]]

    # Get dates by CDR code
    store = HDFStore('NonSettlementDates.h5')
    df = store.select(code)
    df = df[(df >= start) & (df <= stop)]
    store.close()

    return df.values


def dropHolidaysFromIndex(ticker, index, offsets=[0]):
    """
    Gets exchange holidays for a given ticker and drops dates from the index for all offsets given
    Eg. offsets = [0,1] will drop all dates on and 1 business days after the holiday
    """
    # Get non settlemtn dates
    holidays = getNonSettlementDates(ticker, index.date.min(), index.date.max())

    # For each offset, drop holidays
    for offset in offsets:
        drop_days = holidays + (offset * pd.tseries.offsets.BDay())
        index = index.drop([index[index.date.tolist().index(x.to_datetime().date())] for x in drop_days.tolist() if x.to_datetime().date() in index.date])

    return index


def getExchangeTimesByTicker(ticker):
    """
    Returns open, close, and timezone for exchange given ticker
    """
    # Read file
    with open('exchangeTimes', 'r') as myfile:
        return msgpack.unpackb(myfile.read())[ticker.split(' ')[1]]


def getTimezoneByTicker(ticker):
    """
    Get pytz timezone string for a given ticker
    """
    return getExchangeTimesByTicker(ticker)['zone']


def getMarketOpen(ticker, date, tzconvert=True):
    """
    Return Timestamp of market open for ticker on a given date
    """
    info = getExchangeTimesByTicker(ticker)
    opentime = time(*info['open'])
    ts = datetime.combine(date, opentime)
    output = pytz.timezone(info['zone']).localize(ts)

    # Convert timezone if specified
    if tzconvert:
        output = output.astimezone(pytz.timezone('America/New_York'))

    return Timestamp(output)


def getMarketClose(ticker, date, tzconvert=True):
    """
    Return Timestamp of market close for ticker
    """
    info = getExchangeTimesByTicker(ticker)
    opentime = time(*info['close'])
    ts = datetime.combine(date, opentime)
    output = pytz.timezone(info['zone']).localize(ts)

    # Convert timezone if specified
    if tzconvert:
        output = output.astimezone(pytz.timezone('America/New_York'))

    return Timestamp(output)


def getVWAPExcludeCodesByTicker(ticker):
    """
    Bloomberg trade codes to ignore during VWAP calculations. Ticker like "MSFT US Equity"
    """
    # Read file
    with open('VWAP_exclude_codes', 'r') as myfile:
        return np.asarray(msgpack.unpackb(myfile.read())[ticker.split(' ')[1]])


def getMarketCloseCodeByTicker(ticker):
    """
    Code to look for when looking for last price at the close. Differs by exchange. Ticker like "MSFT US Equity"

    Returns: string
    """
    # Read file
    with open('E:/dev/Pheonix/market_close_codes', 'r') as myfile:
        return msgpack.unpackb(myfile.read())[ticker.split(' ')[1]]


def getTickDataPath(mode='prod'):

    path = '//nj1ppfstd01/TickData/'

    if mode == 'test':
        path += 'Test/'

    return path


def getTickData(ticker, start=None, end=None, code=None):
    """
    Query tick data for equities based on start and end datetimes/Timestamps. Tickers passed like 'MSFT US Equity'
    """
    # Convert to pandas timestamp, default New York
    if start: start = Timestamp(start, tz=pytz.timezone('America/New_York'))
    if end: end = Timestamp(end, tz=pytz.timezone('America/New_York'))

    # File path and name
    file_path = getTickDataPath() + ticker.replace(' ', '_').replace('/', '-') + '.h5'

    # Name of dataset within HDF5 file
    dataset_path = 'ticks'

    # Open Store
    store = HDFStore(file_path)

    # If no params specified, return entire dataset
    if all(v is None for v in [start, end, code]):
        output = store.select(dataset_path)
        store.close()
        return output

    # Build query
    where = ''

    # If start specified
    if start:
        where += '(index >= "' + str(start) + '")'

    # If end specified
    if end:
        # If appending, add logic
        if where: where += ' & '
        where += '(index < "' + str(end) + '")'

    # If codes specified
    if code:
        # If appending, add logic
        if where: where += ' & '
        where += '(Codes =' + code + ')'

    output = store.select(dataset_path, where=where)
    store.close()
    return output


def getAvgDailyVol(data):
    """
    Get Average Daily Volume given tick data series for equity
    """
    return data.Size.resample('D', how=np.sum).dropna().mean()


def getVWAP(ticker, start, end, data=None):
    """
    Returns -1 if not trades occurred during the period
    """
    start = Timestamp(start)

    # Go through 1s before end
    end = Timestamp(end - timedelta(seconds=1))

    # If data supplied
    if data is not None:
        # trades = data[start:end]
        trades = data[(data.index > start) & (data.index < end)]
    else:
        # Load trades
        trades = getTickData(ticker, start=start, end=end)

    # If no trades occurred during the time period
    if trades.empty: return 0

    # Filter VWAP codes
    trades = trades[trades.VWAP_Include]

    # If no trades occurred during the time period
    if trades.empty: return 0

    # Calculate VWAP
    return (trades.Price * trades.Size).sum() / float(trades.Size.sum())


def getMarketClosePrices(ticker, index=None, data=None):
    """
    Returns DataFrame with Market close prices for specified dates

    Ticker: "MSFT US Equity"
    Index: Pandas DatetimeIndex
    """
    # Get market close code for ticker
    code = getMarketCloseCodeByTicker(ticker)

    # If data supplied
    if data is not None:

        # If datetime index supplied
        if index is not None:
            return data[data.Codes == code].groupby(level=0).last().reindex(index, method='pad').Price

        else:
            return data[data.Codes == code].Price

    # Get ticks
    ticks = getTickData(ticker, start=index.date.min(), end=(index.date.max() + timedelta(days=1)), code=code)

    # Get unique trades by time 
    ticks = ticks.ix[np.unique(ticks.index, return_index=True)[1]]
    return ticks.reindex(index, method='pad').Price


def getPrices(ticker, index, data=None, VWAP_only=False):
    """
    Returns DataFrame with last prices for specified datetimes

    Ticker: "MSFT US Equity" / "GBP Curncy"
    Dates: Array-like
    """
    # If data supplied
    if data is not None:
        if VWAP_only: data = data[data.VWAP_Include]
        data = data.ix[np.unique(data.index, return_index=True)[1]]
        return data.reindex(index, method='pad').Price

    # Convert index to datetimeindex
    index = DatetimeIndex(index, tz=pytz.timezone('America/New_York'))

    # If just one day, need get through next day
    if len(index) == 1:
        ticks = getTickData(ticker, start=index.date[0], end=(index.date + timedelta(days=1))[0])

    else:
    # Get ticks
        ticks = getTickData(ticker, start=index.date.min(), end=(index.date.max() + timedelta(days=1)))

    # Get unique trades by time
    ticks = ticks.ix[np.unique(ticks.index, return_index=True)[1]]
    if VWAP_only: ticks = ticks[ticks.VWAP_Include]
    return ticks.reindex(index, method='pad').Price


def updateTickData(processMethod='multiprocess', core_multiplier=3):
    """
    Update Tick Data to HDF5 files for selected securities
    """
    ADRlist = pd.DataFrame.from_csv('ADR_test.csv')
    securitylist = []

    # Add ADRs, ORDs, FX, Futures
    securitylist += ADRlist.index.values.tolist()
    securitylist += ADRlist.ORD.tolist()
    securitylist += ADRlist.FX.unique().tolist()
    securitylist += ADRlist.Futures.unique().tolist()

    print "****** STARTING TICK DATA UPDATE ******"

    # If sequential, update one after the other
    if processMethod == 'simple':
        # First equity names
        for security in securitylist:
            BBG.updateHistoricalTickData(security)

    # If multiprocess, use multiprocessing library to speed up
    if processMethod == 'multiprocess':
        numProcesses = cpu_count() * core_multiplier

        # Start multiprocessing Pool and map updates to pool
        p = Pool(processes=numProcesses)
        p.map(BBG.updateHistoricalTickData, securitylist)


def inializeTickMemorySubscriptions(universe):

    memory = intradayTickMemory()

    event_handler = partial(BBG_message_to_mem, memory=memory)
    Thread(target=BBG.startRTSubscriptions, args=(universe, event_handler)).start()

    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind('tcp://*:8080')

    while True:
        ADR = sock.recv()
        ORD = getORD(ADR)
        FX = getADRFX(ADR)
        Futures = getFutures(ADR)
        Ratio = getADRRatio(ADR)

        message = ''

        try:
            adr_price = memory.memdict[ADR].Price[-1]
            message += str(adr_price)
        except:
            print 'NO ADR'

        try:
            ord_prices = memory.memdict[ORD]
            ord_price = ord_prices.Price[-1]
            message += ',' + str(ord_price)
        except:
            print 'NO ORD'
            sock.send(message)
            continue
        close_code = getMarketCloseCodeByTicker(ORD)
        close_prices = ord_prices[ord_prices.Codes == close_code]
        if not close_prices.empty:
            ord_close = close_prices.Price[-1]

            message += ',' + str(ord_close)

            ord_close_time = close_prices.index[-1]

            fx_price = memory.memdict[FX].Price[-1]
            adr_premium = calcADRPremium(adr_price, ord_close, fx_price, Ratio, FX)
            message += ',' + str(adr_premium)

            futures_price = memory.memdict[Futures].Price[-1]
            futures_close_price = getMarketClosePrices(Futures, index=[ord_close_time], data=memory.memdict[Futures])
            futures_ret = (futures_price / futures_close_price) - 1

            message += ',' + str(futures_ret)

            indicator = adr_premium - futures_ret

            message += ',' + str(indicator)

        sock.send(message)
