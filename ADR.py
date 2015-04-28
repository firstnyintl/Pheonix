import pandas as pd
import pytz
from datetime import datetime, time, date
import BBG
import pdb


def getADRlist():
    """
    Load list of ADRs from CSV to ADREquivs
    """
    return pd.DataFrame.from_csv('ADR.csv')


def addOrds(ADRlist):
    """
    Returns DataFrame with ORD equivalents and Ratios
    """
    # Bloomberg fields for ORD equivalent and ADR ratio
    fields = ['ADR_UNDL_TICKER', 'ADR_SH_PER_ADR']

    # Get values from Bloomberg
    ords = BBG.getFields(ADRlist.ADR, fields)

    # Add new data to existing frame
    output = ADRlist.join(ords, on='ADR')

    # Rename columns
    output.columns = ['ADR', 'Bid', 'Offer', 'ORD', 'Ratio']

    # Add equity qualifier so Bloomberg data can be retrieved
    output.ORD = output.ORD + ' Equity'

    return output


def getVWAPtimes(country="Japan"):
    """
    Get VWAP start and end date parameters depending on country
    Returns:
    -- start, end       (strings)
    """
    reference = {
        'Japan': {
            'start': time(9, 0),
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
            }
        }

    # Get today's date
    today = date.today()

    # Set local timezone
    local_zone = pytz.timezone('America/New_York')

    # Build datetimes
    start = reference[country]['start']
    start = datetime.combine(today, start)
    end = reference[country]['end']
    end = datetime.combine(today, end)

    # Convert to correct timezone
    timezone = pytz.timezone(reference[country]['zone'])
    start = timezone.localize(start).astimezone(local_zone)
    end = timezone.localize(end).astimezone(local_zone)

    # Extract date and convert to BBG format
    dt = start.date().strftime("%m/%d/%Y")

    # Extract times and convert to BBG format
    start = start.time().strftime("%H:%M:%S")
    end = end.time().strftime("%H:%M:%S")

    return start, end, dt


def getVWAP(securities, start, end, dt):
    """
    Get vwap on given date, and given start time, for list of securities
    """
    # Main Bloomberg field
    FIELDS = 'VWAP'

    # VWAP override fields
    OVERRIDE_FIELDS = ['VWAP_START_TIME', 'VWAP_END_TIME', 'VWAP_DT']

    # VWAP override values
    OVERRIDE_VALUES = [start, end, dt]

    # Get Bloomberg VWAP data
    output = BBG.getFieldsOverride(securities, FIELDS, OVERRIDE_FIELDS, OVERRIDE_VALUES)

    return output


def yesterdayWorkingReport(bps=30):
    """
    Returns Names that would have worked yesterday
    Takes BPS threshold
    """

    def addORDVWAP(data):

        # Get start/end time and date parameters
        start, end, dt = getVWAPtimes(country='Japan')

        # Get VWAP data from BBG and add to DyataFrame
        vwap = getVWAP(data.ORD, start, end, dt)
        output = data.join(vwap, on='ORD')
        return output

    # Load ADRS
    ADRlist = getADRlist()

    # Add ORD equivalents and Ratios
    data = addOrds(ADRlist)

    # Add yesterday's last trade price for ADRs
    yest_last_trade_data = BBG.getFields(data.ADR, "YEST_LAST_TRADE")
    data = data.join(yest_last_trade_data, on='ADR')
    data.columns = ['ADR', 'Bid', 'Offer', 'ORD', 'Ratio', 'Yesterday']

    # Add ORD VWAP
    data = addORDVWAP(data)

    # Add ORD in $
    JPY_last = BBG.getSingleField('JPY Curncy', 'YEST_LAST_TRADE')
    data['ORD USD'] = data.VWAP / JPY_last * data.Ratio

    # Calculate spread column
    data['Gross Spread'] = ((data['ORD USD'] / data.Yesterday) - 1) * -100

    # Adjust spread column for turns
    data['Adj. Price'] = None
    data['Adj. Price'][data['Gross Spread'] < 0] = data.Yesterday - data.Bid
    data['Adj. Price'][data['Gross Spread'] > 0] = data.Yesterday + data.Offer

    # Calculate adjusted spread
    data['Net Spread'] = ((data['ORD USD'] / data['Adj. Price']) - 1) * -100

    output = data[data['Net Spread'].abs() > bps/100.]
    print output[['ADR', 'Net Spread']]

if __name__ == '__main__':
    yesterdayWorkingReport()
