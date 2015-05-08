import pandas as pd
import multiprocessing
from datetime import datetime, time, date, timedelta
from BBG import updateHistoricalTickData, updateHistoricalFXData, getExchangeHolidaysByTickers
import pdb

VWAPtimes = {
    'Japan': {
        'start': time(9, 00),
        'end': time(9, 20),
        'zone': 'Asia/Tokyo'
    },
    'Hong Kong': {
        'start': time(9, 30),
        'end': time(10, 00),
        'zone': 'Asia/Hong_Kong'
    },
    'Australia': {
        'start': time(9, 00),
        'end': time(10, 00),
        'zone': 'Australia/Sydney'
    },
    'Europe': {
        'start': time(9, 00),
        'end': time(11, 00),
        'zone': 'Europe/Berlin'
    },
    'UK': {
        'start': time(9, 00),
        'end': time(11, 00),
        'zone': 'Europe/London'
    },
    'South Africa': {
        'start': time(9, 00),
        'end': time(11, 00),
        'zone': 'Africa/Johannesburg'
    }
}


def updateTickData():

    ADRlist = pd.DataFrame.from_csv('ADR_test.csv')

    # Create list of securities to update tick data for
    securitylist = []

    # Add ADRs
    securitylist += ADRlist.index.values.tolist()

    # Add ORDs
    securitylist += ADRlist.ORD.tolist()

    # Add FX
    fxlist = ADRlist.FX.unique().tolist()
    fxlist = [c + ' Curncy' for c in fxlist]

    # Print starting message
    print "****** STARTING TICK DATA UPDATE ******"

    # List of threads
    jobs = []

    # Pull tick data for equities
    for security in securitylist:

        # Create new thread to update tick data
        p = multiprocessing.Process(target=updateHistoricalTickData, args=(security,))

        # Add thread to threads
        jobs.append(p)

    # Pull minute bar close data for currencies
    for fx in fxlist:

        # Create new thread to update tick data
        p = multiprocessing.Process(target=updateHistoricalFXData, args=(fx,))

        # Add thread to threads
        jobs.append(p)

    # Start threads
    for j in jobs:
        j.start()


def backtest():

    # Load ADR / ORD / Ratios from csv
    universe = pd.DataFrame.from_csv('ADR_test.csv')

    # Set backtest start date (100 days back)
    start_date = date.today() - timedelta(days=100)

    # Set backtest end date to yesterday
    end_date = date.today() - timedelta(days=1)

    # Timezone
    timezone = 'America/New_York'

    # Build index for dataframe
    date_range_index = pd.bdate_range(start_date, end_date, tz=timezone)

    # Get holidays for ORD and ADR
    tickers = universe.index.tolist() + universe.ORD.tolist()
    holidays = getExchangeHolidaysByTickers(tickers, start_date, end_date)

    # COMPUTE PREMIUM/DISCOUNT INDICATOR
    # Compute indicator on business Days only
    indicator_freq = 'B'

    # Time of day to compute indicator
    indicator_time = time(15, 30)

    # Build datetime objects and pandas DataFrame
    indicator_start = datetime.combine(start_date, indicator_time)
    indicator_end = datetime.combine(end_date, indicator_time)
    indicator_index = pd.date_range(indicator_start, indicator_end, freq=indicator_freq, tz=timezone)

    # Get ADR name and DB info
    ADR_index = 0
    ADR_name = universe.ix[ADR_index].name
    ADR_file_path = 'TickData/' + ADR_name.replace(' ', '_') + '.h5'
    table_path = 'ticks'

    # Get ORD name and DB info
    ORD_name = universe.ix[ADR_index].ORD
    ORD_file_path = 'TickData/' + ORD_name.replace(' ', '_') + '.h5'
    table_path = 'ticks'

    # Get ORD name and DB info
    FX_name = universe.ix[ADR_index].FX + ' Curncy'
    FX_file_path = 'TickData/' + FX_name.replace(' ', '_') + '.h5'
    table_path = 'ticks'

    # Drop indicator dates that coincide with exchange holidays for both ADR and Ord
    ADR_holidays = holidays[ADR_name].tolist()
    ORD_holidays = holidays[ORD_name].tolist()
    all_holidays = ADR_holidays + ORD_holidays
    # Get indeces of holidays and delete datetimes from indicator_index
    for h in all_holidays:
        indx = indicator_index.date.tolist().index(h)
        indicator_index = indicator_index.drop(indicator_index[indx])

    # Load ADR, ORD, FXtrades history from DB 
    ADR_trades = pd.read_hdf(ADR_file_path, table_path)
    ORD_trades = pd.read_hdf(ORD_file_path, table_path)
    FX_trades = pd.read_hdf(FX_file_path, table_path)

    # If multiple trades happened at the same exact time, TAKE THE LAST
    ADR_trades = ADR_trades.groupby(level=0).last()
    ORD_trades = ORD_trades.groupby(level=0).last()
    FX_trades = FX_trades.groupby(level=0).last()

    # Indicator needs to go back how many days
    days_before_start = 0

    # Set filters for trade history range (index is THROUGH date, need to add 1 day to enddate)
    start_filter = date_range_index[0] - timedelta(days=days_before_start)
    end_filter = date_range_index[-1] + timedelta(days=1)

    # Get index for subset of data needed
    ADR_ix = ADR_trades[start_filter:end_filter].index
    ORD_ix = ORD_trades[start_filter:end_filter].index
    FX_ix = FX_trades[start_filter:end_filter].index

    # Get index of closest trades to indicator timedates
    ADR_ix = indicator_index.map(lambda t: ADR_ix.asof(t))
    ORD_ix = indicator_index.map(lambda t: ORD_ix.asof(t))
    FX_ix = indicator_index.map(lambda t: FX_ix.asof(t))

    # Get trades
    ADR_trades = ADR_trades.ix[ADR_ix.tolist()]
    ORD_trades = ORD_trades.ix[ORD_ix.tolist()]
    FX_trades = FX_trades.ix[FX_ix.tolist()]

    # Create indicator DataFrame
    indicator = pd.DataFrame(index=indicator_index)
    indicator['ADR'] = ADR_trades.Price.values
    indicator['ORD'] = ORD_trades.Price.values
    indicator['FX'] = FX_trades.Price.values

    # Calculate indicator
    indicator['ADR Equiv'] = indicator['ORD'] / universe.ix[ADR_index].Ratio / indicator['FX']
    indicator['ADR-Premium-abs'] = indicator['ADR'] - indicator['ADR Equiv']
    indicator['ADR-Premium-pct'] = indicator['ADR-Premium-abs'] / indicator['ADR Equiv'] * 100

    # GENERATE SIGNALS
    # Set threshold to 1%
    short_threshold = 1
    long_threshold = -1
    short_signal_index = indicator[indicator['ADR-Premium-pct'] > short_threshold].index
    long_signal_index = indicator[indicator['ADR-Premium-pct'] < long_threshold].index

    # Build signals dataframe
    signals = pd.DataFrame(columns=['B/S ADR'], index=short_signal_index+long_signal_index)
    signals['B/S ADR'].ix[short_signal_index] = 'S'
    signals['B/S ADR'].ix[long_signal_index] = 'B'

    pdb.set_trace()


if __name__ == '__main__':
    backtest()
