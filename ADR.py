import pandas as pd
from datetime import datetime, timedelta
import BBG


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
    be advised: Daylight savings needs to be acconuted for
    """
    if country == "Japan":
        return "20:00:00", "20:20:00"
    if country == 'Hong Kong':
        return "21:30:00", "22:00:00"
    if country == 'Australia':
        return '20:00:00', '21:00:00'
    if country == 'Europe':
        return '03:00:00', '06:30:00'
    if country == 'United Kingdom':
        return '03:00:00', '06:30:00'
    if country == 'United States':
        return '15:20:00', '15:59:30'
    
def getVWAP(securities, start, end, date):
    """
    Get vwap on given date, and given start time, for list of securities
    """
    # Main Bloomberg field
    FIELDS = 'VWAP'

    # VWAP override fields
    OVERRIDE_FIELDS = ['VWAP_START_TIME', 'VWAP_END_TIME', 'VWAP_DT']

    # VWAP override values
    OVERRIDE_VALUES = [start, end, date]

    # Get Bloomberg VWAP data
    output = BBG.getFieldsOverride(securities, FIELDS, OVERRIDE_FIELDS, OVERRIDE_VALUES)

    return output


def yesterdayWorkingReport(bps=30):
    """
    Returns Names that would have worked yesterday
    Takes BPS threshold
    """

    def addORDVWAP(data):
        # Get yesterday's timedate
        yesterday = datetime.now() - timedelta(days=1)
        # Convert to BBG format
        yesterday = yesterday.strftime("%m/%d/%Y")
        start, end = getVWAPtimes(country='Japan')
        vwap = getVWAP(data.ORD, start, end, yesterday)
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
