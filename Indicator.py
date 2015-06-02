from pytz import timezone
from datetime import datetime, time
import pandas as pd
from ADR import getORD, getADRFX, getADRRatio, calcADRPremium
from Data import dropHolidaysFromIndex, getPrices, getMarketClosePrices


def ADR_Premium(ticker, start, end, frequency='B', time_of_day=[15, 30], ADR_data=None, ORD_data=None, FX_data=None):
    """
    Ticker: "MSFT US Equity'
    Start, End: datetime.date
    Data: TickData DataFrame (Optional)-- if passed, will calculate from this, otherwise will read from DB
    """
    def buildIndex():
        """
        Build Empty DataFrame with correct DateTimeIndex index. Accounts for Holidays
        """
        # Time of day to calculate indicator at
        calc_time = time(*time_of_day)

        # Get start and end of indicator
        start_dt = datetime.combine(start, calc_time)
        end_dt = datetime.combine(end, calc_time)

        # Build index for dataframe
        index = pd.date_range(start_dt, end_dt, freq=frequency, tz=timezone('America/New_York'))

        # Drop ADR holidays from index
        index = dropHolidaysFromIndex(ticker, index)

        # Drop dates on ORD holidays
        index = dropHolidaysFromIndex(getORD(ticker), index)

        # Create indicator DataFrame
        return pd.DataFrame(index=index)

    # Create Empty Indicator Object
    indicator = buildIndex()

    # Get last ADR trade as of indicator dates and add to dataframe
    indicator['ADR'] = getPrices(ticker, indicator.index, data=ADR_data)

    # Get ORD last prices as of exchange close and add to indicator dataframe 
    indicator['ORD'] = getMarketClosePrices(getORD(ticker), index=indicator.index, data=ORD_data)

    # Get last FX trade as of indicator dates and add to dataframe
    indicator['FX'] = getPrices(getADRFX(ticker), indicator.index, data=FX_data)

    # Calculate Premium
    indicator[ADR_Premium.__name__] = indicator.apply(lambda row: calcADRPremium(row['ADR'], row['ORD'], row['FX'], getADRRatio(ticker), getADRFX(ticker)), axis=1)

    # Return Indicator
    return indicator[ADR_Premium.__name__]
