import pandas as pd
import multiprocessing
from datetime import datetime, time, date, timedelta
from BBG import updateHistoricalTickData
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
    fx = ADRlist.FX.unique().tolist()
    securitylist += [c + ' Curncy' for c in fx]

    # Print starting message
    print "****** STARTING TICK DATA UPDATE ******"

    # List of threads
    jobs = []

    # Pull data from Bloomberg
    for security in securitylist:

        # Create new thread to update tick data
        p = multiprocessing.Process(target=updateHistoricalTickData, args=(security,))

        # Add thread to threads
        jobs.append(p)

    # Start threads
    for j in jobs:
        j.start()


def backtest():

    # Load ADR / ORD / Ratios from csv
    ADRlist = pd.DataFrame.from_csv('ADR_test.csv')

    # Set backtest start date (100 days back)
    start_date = date.today() - timedelta(days=100)

    # Set backtest end date to yesterday
    end_date = date.today() - timedelta(days=1)

    # Build index for dataframe
    date_range_index = pd.bdate_range(start_date, end_date)

    # Build dataframe
    time_df = pd.DataFrame(index=date_range_index)

    # Time to generate signal (read premium)
    signal_time = time(15, 30)
    signal_timezone = 'America/New_York'
    signal_range_start = datetime.combine(start_date, signal_time)
    signal_range_end = datetime.combine(end_date, signal_time)
    time_df['Signals'] = pd.date_range(signal_range_start, signal_range_end, freq='B', tz=signal_timezone)

    # Time to start entry (VWAP)
    entry_start_time = time(15, 30)
    entry_start_timezone = 'America/New_York'
    entry_start_range_start = datetime.combine(start_date, entry_start_time)
    entry_start_range_end = datetime.combine(end_date, entry_start_time)
    time_df['Entry Start'] = pd.date_range(entry_start_range_start, entry_start_range_end, freq='B', tz=entry_start_timezone)

    # Time to start entry (VWAP)
    entry_end_time = time(16, 00)
    entry_end_timezone = 'America/New_York'
    entry_end_range_start = datetime.combine(start_date, entry_end_time)
    entry_end_range_end = datetime.combine(end_date, entry_end_time)
    time_df['Entry End'] = pd.date_range(entry_end_range_start, entry_end_range_end, freq='B', tz=entry_end_timezone)

    # Add exit times (VWAP)
    for key, value in zip(VWAPtimes.keys(), VWAPtimes.values()):
        exit_start_time = value['start']
        exit_end_time = value['end']
        exit_timezone = value['zone']
        exit_start_range_start = datetime.combine(start_date, exit_start_time)
        exit_start_range_end = datetime.combine(end_date, exit_start_time)
        exit_end_range_start = datetime.combine(start_date, exit_end_time)
        exit_end_range_end = datetime.combine(end_date, exit_end_time)
        exit_start_local = pd.date_range(exit_start_range_start, exit_start_range_end, freq='B', tz=exit_timezone)
        exit_start_convert = exit_start_local.tz_convert('America/New_York')
        exit_end_local = pd.date_range(exit_end_range_start, exit_end_range_end, freq='B', tz=exit_timezone)
        exit_end_convert = exit_end_local.tz_convert('America/New_York')
        time_df[key+' Exit Start'] = exit_start_convert
        time_df[key+' Exit End'] = exit_end_convert

    pdb.set_trace()


if __name__ == '__main__':
    updateTickData()
