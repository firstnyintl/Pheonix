import pdb
import pytz
import copy
import itertools
import multiprocessing
from concurrent import futures
from datetime import timedelta, date, time, datetime
import pandas as pd
import Indicator
from Data import getVWAP, getPrices, getExchangeTimesByTicker, getTickData, getNonSettlementDates
from ADR import getADRFX, getADRRatio, getORD, calcADREquiv, getUniverse, getWorstCaseTurn, getADRTable


ADR_basic = {
    'Signal': {
        'Premium': {
            'indicator': 'ADR_Premium',
            'logic': '>',
            'value': .5
        },
        'Discount': {
            'indicator': 'ADR_Premium',
            'logic': '<',
            'value': -.5,
        }
    },
    'Execution': {
        'ORD_VWAP_hours_after_open': {
            'value': 2
        }
    }
}


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


def buildSignals(ticker, date_range, conditions, ADR_data=None, ORD_data=None, FX_data=None, ADR_table=None):
    """
    Builds signals DataFrame
    """
    # Get indicator names
    indicator_names = conditions['indicator'].unique()

    # Get indicators
    indicators = pd.DataFrame()
    for name in indicator_names:
        indicator = getattr(Indicator, name)
        indicators[name] = indicator(ticker, date_range.min().date(), date_range.max().date(), ADR_data=ADR_data, ORD_data=ORD_data, FX_data=FX_data, ADR_table=ADR_table)

    # Create empty signals list
    signals = pd.Series(index=indicators.index)

    # Loop through all conditions
    for classification, (indicator, logic, value) in conditions.iterrows():

        # Create bool vector where conditions are true
        bool_vector = indicators.eval(' '.join([indicator, logic, str(value)]))

        # Add signals to signals Seriesy
        signals[bool_vector] = classification

    # Get rid of NaN values
    signals = signals.dropna()

    return signals


def buildSignalRules(rules):
    """
    Returns a dataframe of conditions that can be used to generate signals
    """

    # Get signal rules from strategy
    signal_rules = rules['Signal']

    return pd.DataFrame(signal_rules).T


def backtest(rules, ticker, days=100, date_range=None, ADR_data=None, ORD_data=None, FX_data=None, ADR_table=None):

    def calculateSpreadsCaptured(signals, ADR_data=None, ORD_data=None, FX_data=None, ADR_table=None):

        # Drop signals where ORD holiday is day after signal
        trade_offset = 1
        holidays = getNonSettlementDates(getORD(ticker, data=ADR_table), signals.index.date.min(), signals.index.date.max())
        holidays = holidays - (trade_offset * pd.tseries.offsets.BDay())
        signals = signals.drop(signals.index[[signals.index.date.tolist().index(x.to_datetime().date()) for x in holidays if x.to_datetime().date() in signals.index.date]])

        # Drop signals yesterday (no tick data today)
        today = (datetime.today() - pd.tseries.offsets.BDay()).to_datetime().date()
        if today in signals.index.date.tolist():
            signals = signals.drop(signals.index[signals.index.date.tolist().index(today)])

        if len(signals) == 0: return None

        # Times to use for VWAP entry
        entry_start_VWAP = time(15, 30)
        entry_end_VWAP = time(16, 00)

        # Build entry trades dataframe
        entry_trades = pd.DataFrame(columns=['Security', 'Type', 'VWAP Start', 'VWAP End', 'Price'], index=signals.index)
        entry_trades['Security'] = ticker
        entry_trades['Type'].ix[signals[signals == 'Discount'].index] = 'Buy'
        entry_trades['Type'].ix[signals[signals == 'Premium'].index] = 'Short'
        entry_trades['VWAP Start'] = map(lambda x: pytz.timezone('America/New_York').localize(datetime.combine(x, entry_start_VWAP)), entry_trades.index)
        entry_trades['VWAP End'] = map(lambda x: pytz.timezone('America/New_York').localize(datetime.combine(x, entry_end_VWAP)), entry_trades.index)
        entry_trades['Price'] = map(lambda x, y: getVWAP(ticker, x, y, data=ADR_data), entry_trades['VWAP Start'], entry_trades['VWAP End'])

        # Get VWAP times and zone
        exchangeTimes = getExchangeTimesByTicker(getORD(ticker, data=ADR_table))

        VWAP_hours_after_open = rules['Execution']['ORD_VWAP_hours_after_open']['value']

        exit_start_VWAP = time(*exchangeTimes['open'])
        exit_end_VWAP = (datetime.combine(date.today(), exit_start_VWAP) + timedelta(hours=VWAP_hours_after_open)).time()
        exit_timezone = exchangeTimes['zone']

        exit_trades = pd.DataFrame(columns=['Security', 'Type', 'VWAP Start', 'VWAP End', 'Price'], index=signals.index)

        # Trade dates (Offset by +1)
        trade_dates = entry_trades.index + pd.tseries.offsets.BDay()
        exit_start_VWAP = map(lambda x: pytz.timezone(exit_timezone).localize(datetime.combine(x, exit_start_VWAP)).astimezone(pytz.timezone('America/New_York')), trade_dates)
        exit_end_VWAP = map(lambda x: pytz.timezone(exit_timezone).localize(datetime.combine(x, exit_end_VWAP)).astimezone(pytz.timezone('America/New_York')), trade_dates)

        # If Long signal, set side to buy, if short signal, set to short
        exit_trades['Security'] = getORD(ticker, data=ADR_table)
        exit_trades['Type'].ix[signals[signals == 'Discount'].index] = 'Sell'
        exit_trades['Type'].ix[signals[signals == 'Premium'].index] = 'Buy'
        exit_trades['VWAP Start'] = exit_start_VWAP
        exit_trades['VWAP End'] = exit_end_VWAP
        exit_trades['Price'] = map(lambda x, y: getVWAP(getORD(ticker, data=ADR_table), x, y, data=ORD_data), exit_trades['VWAP Start'], exit_trades['VWAP End'])
        exit_trades['FX'] = getPrices(getADRFX(ticker, data=ADR_table), exit_trades['VWAP End'], data=FX_data).values
        exit_trades['ADR Equiv'] = exit_trades.apply(lambda row: calcADREquiv(row['Price'], row['FX'], getADRRatio(ticker, data=ADR_table), getADRFX(ticker, data=ADR_table)), axis=1)

        # Combine in one dataframe
        result = pd.DataFrame(index=entry_trades.index)

        # Bring over info from other trades
        result['Buy/Sell ADR'] = entry_trades['Type']
        result['ADR Entry'] = entry_trades['Price']
        result['ADR Equiv Exit'] = exit_trades['ADR Equiv']
        result['Gross Profit'] = result['ADR Equiv Exit'] - result['ADR Entry']
        result['Gross Profit'][result['Buy/Sell ADR'] == 'Short'] * -1
        result['Turn (worst case)'] = result.apply(lambda row: getWorstCaseTurn(ticker, row['Buy/Sell ADR'], price=row['ADR Equiv Exit']), axis=1)
        result['Net Profit'] = result['Gross Profit'] - result['Turn (worst case)']

        return result

    def runReport(result, start_time):
        """
        Compile stats and summary on results
        """
        # Check if empty
        empty = False
        if result is None: empty = True

        # Get end_time
        end_time = datetime.now()

        # Drop days with zero liquidity
        zero_liquidity_trades = 0
        if not empty:
            zero_liquidity_trades = result[(result['ADR Entry'] == 0) | (result['ADR Equiv Exit'] == 0)]
            result = result.drop(zero_liquidity_trades.index)
            zero_liquidity_trades = len(zero_liquidity_trades)

        report = {
            'Ticker': ticker,
            'Start Date': date_range[0],
            'End Date': date_range[-1],
            'Long ADR Trades': 0 if empty else len(result[result['Buy/Sell ADR'] == 'Buy']),
            'Short ADR Trades': 0 if empty else len(result[result['Buy/Sell ADR'] == 'Short']),
            'Avg. Gross Profit': 0 if empty else result['Gross Profit'].mean(),
            'Avg. Net Profit': 0 if empty else result['Net Profit'].mean(),
            'Max Net Profit': 0 if empty else result['Net Profit'].max(),
            'Min Net Profit': 0 if empty else result['Net Profit'].min(),
            'Average Turn Cost': 0 if empty else result['Turn (worst case)'].mean(),
            'Number of 0 liquidity trades': zero_liquidity_trades,
            # 'Strategy': rules
            'Speed (s)': (end_time - start_time).total_seconds(),
            'Premium threshold %': rules['Signal']['Premium']['value'],
            'Discount threshold %': rules['Signal']['Discount']['value'],
            'VWAP ORD # hours after open': rules['Execution']['ORD_VWAP_hours_after_open']['value']
        }
        return pd.Series(report)

    def runBackTest(ADR_data, ORD_data, FX_data, ADR_table):

        # Take Strategy, build signal rules for signal generation 
        signal_rules = buildSignalRules(rules)

        if ADR_table is None:
            ADR_table = getADRTable()
        if ADR_data is None:
            ADR_data = getTickData(ticker, date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))
        if ORD_data is None:
            ORD_data = getTickData(getORD(ticker, data=ADR_table), date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))
        if FX_data is None:
            FX_data = getTickData(getADRFX(ticker, data=ADR_table), date_range.date.min(), (date_range.date.max() + pd.tseries.offsets.BDay()))

        # Build Signals from conditions
        signals = buildSignals(ticker, date_range, signal_rules, ADR_data=ADR_data, ORD_data=ORD_data, FX_data=FX_data, ADR_table=ADR_table)

        # If made trades
        if len(signals) != 0:
            return calculateSpreadsCaptured(signals, ADR_data=ADR_data, ORD_data=ORD_data, FX_data=FX_data, ADR_table=ADR_table)

        else:
            return None

    start_time = datetime.now()

    # Build date range
    if date_range is None:
        date_range = buildDateRange(days)

    # Run backtest results
    result = runBackTest(ADR_data, ORD_data, FX_data, ADR_table)

    # Calculate report
    report = runReport(result, start_time)

    print report
    print '\n'

    return report


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
