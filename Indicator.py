from pytz import timezone
from datetime import datetime, time
import pandas as pd
import numpy as np
from ADR import getORD, getADRFX, getADRRatio, calcADRPremium
from Data import dropHolidaysFromIndex, getPrices, getMarketClosePrices, getTickData

class ADR_Premium:

    # This is main security used to build indicator
    buildingSecurity = 'ADR'

    def __init__(self, ADR, start=None, end=None, frequency='B', time_of_day=[15, 30], data=None):

        self.ADR = ADR
        self.ORD = getORD(self.ADR)
        self.FX = getADRFX(self.ADR)
        self.Ratio = getADRRatio(self.ADR)
        self.start = start
        self.end = end
        self.frequency = frequency
        self.time_of_day = time(*time_of_day)
        self.data = data
        self.name = 'ADR_Premium'

    def loadData(self):
        """
        Loads data required to build indicator (if none was passed)
        """
        if self.data is None:
            self.ADR_data = getTickData(self.ticker, self.start, self.end + pd.tseries.offsets.BDay())
            self.ORD_data = getTickData(self.ORD, self.start, self.end + pd.tseries.offsets.BDay())
            self.FX_data = getTickData(self.FX, self.start, self.end + pd.tseries.offsets.BDay())
        else:
            self.ADR_data = self.data[self.ADR]
            self.ORD_data = self.data[self.ORD]
            self.FX_data = self.data[self.FX]

    def build(self):
        """
        Ticker: "MSFT US Equity'
        Start, End: datetime.date
        Data: TickData DataFrame (Optional)-- if passed, will calculate from this, otherwise will read from DB
        """
        def buildIndex():
            """
            Build Empty DataFrame with correct DateTimeIndex index. Accounts for Holidays
            """
            # Get start and end of indicator
            start_dt = datetime.combine(self.start, self.time_of_day)
            end_dt = datetime.combine(self.end, self.time_of_day)

            # Build index for dataframe
            index = pd.date_range(start_dt, end_dt, freq=self.frequency, tz=timezone('America/New_York'))

            # Drop ADR and ORD holidays
            index = dropHolidaysFromIndex(self.ADR, index)
            index = dropHolidaysFromIndex(self.ORD, index)

            # Create indicator DataFrame
            return pd.DataFrame(index=index)

        # Get data
        self.loadData()

        # Create Empty Indicator Object
        indicator = buildIndex()

        # Get last ADR trade as of indicator dates and add to dataframe
        indicator['ADR'] = getPrices(self.ADR, indicator.index, data=self.ADR_data)

        # Get ORD last prices as of exchange close and add to indicator dataframe 
        indicator['ORD'] = getMarketClosePrices(self.ORD, index=indicator.index, data=self.ORD_data)

        # Get last FX trade as of indicator dates and add to dataframe
        indicator['FX'] = getPrices(self.FX, indicator.index, data=self.FX_data)

        # Calculate Premium
        indicator[self.name] = indicator.apply(lambda row: calcADRPremium(row['ADR'], row['ORD'], row['FX'], self.Ratio, self.FX), axis=1)

        # Return Indicator
        return indicator[self.name]

    @property
    def requiredSecurities(self):
        """
        Returns dictionary of functions required to load data for indicator
        """
        return np.asarray([self.ADR, self.ORD, self.FX])
