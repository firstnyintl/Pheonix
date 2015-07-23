import pdb
import pytz
import copy
import itertools
import multiprocessing
from concurrent import futures
from datetime import timedelta, date, time, datetime
import pandas as pd
import Indicator
from Data import getVWAP, getPrices, getExchangeTimesByTicker, getTickData, getTimezoneByTicker, getMarketClose, getMarketOpen
from ADR import getADRFX, getORD, calcADREquiv, getUniverse, getWorstCaseTurn, getADRTable
from Strategy import ADR_basic
from Order import buildOrder


def buildDateRange(days):
    """
    Returns initial date range to base backtest on
    """
    # Set inital range INCLUSIVE OF t-1 business days (due to tick data availability)
    end_date = date.today() - pd.tseries.offsets.BDay()

    start_date = end_date - timedelta(days=days)

    # Set timezone to New York
    timezone = 'America/New_York'

    # Build index for dataframe (EXCLUDES NON-BUSINESS DAYS)
    date_range_index = pd.bdate_range(start_date, end_date, tz=timezone)

    return date_range_index


def buildSignals(strategy, date_range, data):
    """
    Builds signals DataFrame
    """
    # Get indicator names and signal rules
    indicator_list = strategy.indicators
    signal_rules = strategy.signalRules

    # Get indicators
    indicators = pd.DataFrame()
    for name in indicator_list:
        indicator = getattr(Indicator, name.split('(')[0])
        referenceTicker = getattr(strategy, indicator.buildingSecurity)
        param = name.split('(')[1].split(')')[0]
        indicator = indicator(referenceTicker, start=date_range.min().date(), end=date_range.max().date(), data=data, time_of_day=param)
        indicators[name] = indicator.build()

    # Create empty signals list
    signals = pd.Series(index=indicators.index)
    indicator_columns = [x.replace('(', '_').replace(')','').replace(':', '_') for x in indicators.columns]
    indicators.columns = indicator_columns

    # Loop through all conditions
    for classification, rules in signal_rules.iterrows():

        rules = pd.DataFrame(rules).T

        bool_vectors = pd.DataFrame(index=signals.index)

        for condition in rules.keys().levels[0]:

            rule = rules[condition]

            indicator, logic, value = tuple(rule.values.tolist()[0])

            # Eval wont work unless modify string
            indicator = indicator.replace('(', '_').replace(')','').replace(':', '_')

            # Create bool vector where conditions are true
            bool_vectors[condition] = indicators.eval(' '.join([indicator, logic, str(value)]))

        # Get where all are true
        signals[bool_vectors.all(axis=1)] = classification

    # Get rid of NaN values
    signals = signals.dropna()

    return signals


def backtest(strategy, days=100, date_range=None, data=None):

    def calculateSpreadsCaptured(strategy, signals, data):

        # Pass signals to strategy to make necessary adjustments
        signals = strategy.processSignals(signals)

        # If no signals after adjusting, return None
        if len(signals) == 0: return None

        signals = pd.DataFrame(signals, columns=['Signal'])

        def getTrades(x, trades):
            x = x[1]
            actions = strategy.rules.Consequents[x.Signal].split(' & ')
            for action in actions:
                act = action.split(' ')
                side = act[0]
                if act[1] == 'ADR': typ = 'entry'
                else: typ = 'exit'
                security = getattr(strategy, act[1])
                algo = act[2].split('(')[0]
                params = act[2].split('(')[1].split(')')[0]
                start_param = params.split(',')[0]
                end_param = params.split(',')[1]

                if algo == 'VWAP':

                    # If start now, set start to signal time
                    if start_param == 'Now':
                        start = x.name

                    # If start is MktOpen, set to market open time
                    if start_param == 'MktOpen':
                        start = getMarketOpen(security, x.name.date())

                    # If end is MktClose, set to mkclose time
                    if end_param == 'MktClose':
                        end = getMarketClose(security, x.name.date())

                    # If end is +min, parse
                    if '+' in end_param:
                        mins = int(end_param.replace('+', ''))
                        end = x.name + timedelta(minutes=mins)

                    price = getVWAP(security, start, end, data=data[security])

                    trades.append([side, typ, security, start, end, price, x.name])

        trades = []
        [getTrades(signal, trades) for signal in signals.iterrows()]
        trades = pd.DataFrame(trades, columns=['Side', 'Type', 'Security', 'Start', 'End', 'Price', 'Signal'])
        trades.index = trades.Signal
        trades = trades.drop('Signal')

        # Build entry trades dataframe
        entry_trades = trades[trades.Type == 'entry']

        exit_trades = trades[trades.Type == 'exit']
        exit_trades['FX'] = getPrices(strategy.FX, exit_trades['End'], data=data[strategy.FX]).values
        exit_trades['ADR Equiv'] = exit_trades.apply(lambda row: calcADREquiv(row['Price'], row['FX'], strategy.Ratio, strategy.FX), axis=1)

        # Combine in one dataframe
        result = pd.DataFrame(index=entry_trades.index)

        # Bring over info from other trades
        result['Buy/Sell ADR'] = entry_trades['Side']
        result['ADR Entry'] = entry_trades['Price']
        result['ADR Equiv Exit'] = exit_trades['ADR Equiv']
        result['Gross Profit'] = result['ADR Equiv Exit'] - result['ADR Entry']
        result['Gross Profit'][result['Buy/Sell ADR'] == 'Short'] * -1
        result['Turn (worst case)'] = result.apply(lambda row: getWorstCaseTurn(strategy.ADR, row['Buy/Sell ADR'], price=row['ADR Equiv Exit']), axis=1)
        result['Net Profit (abs)'] = result['Gross Profit'] - result['Turn (worst case)']
        result['Net Profit (bps)'] = result['Net Profit (abs)'] / result['ADR Entry'] * 10000

        return result

    # Time backtest
    start_time = datetime.now()

    # Build date range
    if date_range is None:
        date_range = buildDateRange(days)

    # Load data
    if data is None:
        data = strategy.loadTickData(date_range)

    # Build Signals from conditions
    signals = buildSignals(strategy, date_range,  data)

    # If made trades
    if len(signals) != 0:
        result = calculateSpreadsCaptured(strategy, signals, data)

    else:
        result = None

    end_time = datetime.now()
    speed = (end_time-start_time).total_seconds()

    return result, data, speed


def buildRulesForOptimization(strategy, optimization):
    """
    Takes strategy dict and optimization dict and returns list of all strategies
    """
    # List to hold paramter names
    params_list = []

    # List to hold value names
    value_lists = []

    # Fill lists from optimization dict
    for header, item in optimization.iteritems():
        for key, value in item.iteritems():
            params_list.append((header, key))
            value_lists.append(value)

    def frange(start, stop, step):
        """
        Generates float range (inclusive)
        """
        output = []
        current_val = start
        while current_val <= stop:
            output.append(current_val)
            current_val += step
        return output

    # Build list of ranges
    range_lists = []
    for r in value_lists:
        range_lists.append(frange(*r))

    # Build all possible combinations
    value_range = list(itertools.product(*range_lists))

    # Build index for combinations Dataframe
    index = pd.MultiIndex.from_tuples(params_list)

    # Build DataFrame with combinations
    combinations = pd.DataFrame(value_range, columns=index)

    def buildRules(row):
        """
        Takes a row from combinations and generates rules
        """
        x = copy.deepcopy(strategy)
        for level in row.index.levels[0]:
            for param in row[level].index:
                x[level][param]['value'] = row[level][param]
        return x

    # Build rules
    rules = []
    for row in combinations.iterrows():
        rule = buildRules(row[1])
        rules.append(rule)

    return rules


def optimize(ticker, strategy=ADR_basic, days=100, ADR_table=None):

    if ADR_table is None:
        ADR_table = getADRTable()

    # # Build date range so only have to compute once
    date_range = buildDateRange(days=100)

    # Load data so only need to load once
    ADR_data = getTickData(ticker, date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))
    ORD_data = getTickData(getORD(ticker, data=ADR_table), date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))
    FX_data = getTickData(getADRFX(ticker, data=ADR_table), date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))

    # Set optimization ranges for paramters
    optimization = {
        'Signal': {
            'Premium': [0, 2.0, 1],
            'Discount': [-2.0, 0, 1]
        },
        'Execution': {
            'ORD_VWAP_hours_after_open': [1, 3, 1]
        }
    }

    # Build rules
    rules = buildRulesForOptimization(strategy, optimization)

    result = pd.DataFrame([backtest(rule, ticker, ADR_data=ADR_data, ORD_data=ORD_data, FX_data=FX_data, date_range=date_range, ADR_table=ADR_table) for rule in rules])

    return result


def globalOptimization(method='simple'):

    # Load ADR Table
    ADR_table = getADRTable()

    universe = getUniverse(data=ADR_table)

    if method == 'simple':
        result = pd.Panel([optimize(stock, ADR_table=ADR_table) for stock in universe])

    pdb.set_trace()


def globalBacktest(days=100, method='multiprocess', load_data='realtime', core_multiplier=1):

    universe = getUniverse()

    if method == 'simple':
        df = pd.DataFrame([backtest(stock, load_data=load_data) for stock in universe])

    if method == 'multiprocess':
        processes = multiprocessing.cpu_count() * core_multiplier
        p = multiprocessing.Pool(processes=processes)
        df = pd.DataFrame([p.map(backtest, universe)])
        # with futures.ProcessPoolExecutor(max_workers=processes) as executor:
        #     df = pd.DataFrame([executor.submit(backtest, stock) for stock in universe])

    # Does not work
    if method == 'thread':
        processes = multiprocessing.cpu_count() * core_multiplier
        with futures.ThreadPoolExecutor(max_workers=processes) as executor:
            df = pd.DataFrame([executor.submit(backtest, stock) for stock in universe])
    # pdb.set_trace()

    return df
