from pytz import timezone
from datetime import datetime, time
import pandas as pd
import numpy as np
import sys, inspect
from ADR import getORD, getADRFX, getADRRatio, calcADRPremium, getFutures
from Data import dropHolidaysFromIndex, getPrices, getMarketClosePrices, getTickData, getExchangeTimesByTicker
import pdb


class ADR_Futures_Spread:

    def calc():


class Return:

    def ():

    def calc():


class ADR_Premium:

    def indicator():

    def calc():


# class ORD_Fut_Rtrn:
#     """
#     Return for futures associated with ORD til given time of day since ORD close, daily frequency
#     """
#     # This is main security used to build indicator
#     buildingSecurity = 'ADR'
#     params = np.asarray([{'name': 'time_of_day', 'type': time}])

#     def __init__(self, ADR, start=None, end=None, frequency='B', time_of_day='15:30', data=None):

#         self.ADR = ADR
#         self.ORD = getORD(self.ADR)
#         self.Futures = getFutures(self.ADR)
#         self.start = start
#         self.end = end
#         self.frequency = frequency
#         tod = time_of_day.split(':')
#         self.time_of_day = time(int(tod[0]), int(tod[1]))
#         self.data = data
#         self.name = 'Futures_Return'

#     def loadData(self):
#         """
#         Loads data required to build indicator (if none was passed)
#         """
#         if self.ADR in self.data:
#             self.ADR_data = self.data[self.ADR]
#         else:
#             self.ADR_data = getTickData(self.ADR, self.start, self.end + pd.tseries.offsets.BDay())
#         if self.Futures in self.data:
#             self.Futures_data = self.data[self.Futures]
#         else:
#             self.Futures_data = getTickData(self.Futures, self.start, self.end + pd.tseries.offsets.BDay())

#     def build(self):
#         """
#         Build indicator
#         """
#         def buildIndex():
#             """
#             Build Empty DataFrame with correct DateTimeIndex index. Accounts for Holidays
#             """
#             # Get start and end of indicator
#             start_dt = datetime.combine(self.start, self.time_of_day)
#             end_dt = datetime.combine(self.end, self.time_of_day)

#             # Build index for dataframe
#             index = pd.date_range(start_dt, end_dt, freq=self.frequency, tz=timezone('America/New_York'))

#             # Drop ADR and ORD holidays
#             index = dropHolidaysFromIndex(self.ADR, index)
#             index = dropHolidaysFromIndex(self.ORD, index)

#             # Create indicator DataFrame
#             return pd.DataFrame(index=index)

#         # Get data
#         self.loadData()

#         # Create Empty Indicator Object
#         indicator = buildIndex()

#         # Get ORD mkt info
#         ORD_mkt_info = getExchangeTimesByTicker(self.ORD)
#         ORD_mkt_close = time(*ORD_mkt_info['close'])
#         ORD_mkt_tz = ORD_mkt_info['zone']

#         # Get indicator index , change to ORD timezone, assign to sepearet index
#         ORD_close_index = indicator.index.tz_convert(ORD_mkt_tz)

#         # Replace time of datetime index
#         ORD_close_index = pd.DatetimeIndex(ORD_close_index.map(lambda x: x.replace(hour=ORD_mkt_close.hour, minute=ORD_mkt_close.minute)))

#         # Change timezone back
#         ORD_close_index = ORD_close_index.tz_convert(timezone('America/New_York'))

#         # Get last Futures trade as of ORD market close
#         indicator['ORD_close_Price'] = getPrices(self.Futures, ORD_close_index, data=self.Futures_data).values

#         # Get last Futures trade as of indicator dates and add to dataframe
#         indicator['TOD_Price'] = getPrices(self.Futures, indicator.index, data=self.Futures_data)

#         # Get return
#         indicator[self.name] = (indicator['TOD_Price'] / indicator['ORD_close_Price']) - 1
#         return indicator[self.name]

#     @property
#     def requiredSecurities(self):
#         """
#         Returns dictionary of functions required to load data for indicator
#         """
#         return np.asarray([self.ADR, self.ORD, self.Futures])


# class ADR_Premium:
#     """
#     ADR Premium at given time of day, daily frequency
#     """

#     # This is main security used to build indicator
#     buildingSecurity = 'ADR'
#     params = np.asarray([{'name': 'time_of_day', 'type': time}])

#     def __init__(self, ADR, start=None, end=None, frequency='B', time_of_day='15:30', data=None):

#         self.ADR = ADR
#         self.ORD = getORD(self.ADR)
#         self.FX = getADRFX(self.ADR)
#         self.Ratio = getADRRatio(self.ADR)
#         self.start = start
#         self.end = end
#         self.frequency = frequency
#         tod = time_of_day.split(':')
#         self.time_of_day = time(int(tod[0]), int(tod[1]))
#         self.data = data
#         self.name = 'ADR_Premium'

#     def loadData(self):
#         """
#         Loads data required to build indicator (if none was passed)
#         """
#         if self.ADR in self.data:
#             self.ADR_data = self.data[self.ADR]
#         else:
#             self.ADR_data = getTickData(self.ADR, self.start, self.end + pd.tseries.offsets.BDay())
#         if self.ORD in self.data:
#             self.ORD_data = self.data[self.ORD]
#         else:
#             self.ORD_data = getTickData(self.ORD, self.start, self.end + pd.tseries.offsets.BDay())
#         if self.FX in self.data:
#             self.FX_data = self.data[self.FX]
#         else:
#             self.FX_data = getTickData(self.FX, self.start, self.end + pd.tseries.offsets.BDay())

#     def build(self):
#         """
#         Build indicator
#         """
#         def buildIndex():
#             """
#             Build Empty DataFrame with correct DateTimeIndex index. Accounts for Holidays
#             """
#             # Get start and end of indicator
#             start_dt = datetime.combine(self.start, self.time_of_day)
#             end_dt = datetime.combine(self.end, self.time_of_day)

#             # Build index for dataframe
#             index = pd.date_range(start_dt, end_dt, freq=self.frequency, tz=timezone('America/New_York'))

#             # Drop ADR and ORD holidays
#             index = dropHolidaysFromIndex(self.ADR, index)
#             index = dropHolidaysFromIndex(self.ORD, index)

#             # Create indicator DataFrame
#             return pd.DataFrame(index=index)

#         # Get data
#         self.loadData()

#         # Create Empty Indicator Object
#         indicator = buildIndex()

#         # Get last ADR trade as of indicator dates and add to dataframe
#         indicator['ADR'] = getPrices(self.ADR, indicator.index, data=self.ADR_data)

#         # Get ORD last prices as of exchange close and add to indicator dataframe 
#         indicator['ORD'] = getMarketClosePrices(self.ORD, index=indicator.index, data=self.ORD_data)

#         # Get last FX trade as of indicator dates and add to dataframe
#         indicator['FX'] = getPrices(self.FX, indicator.index, data=self.FX_data)

#         # Calculate Premium
#         indicator[self.name] = indicator.apply(lambda row: calcADRPremium(row['ADR'], row['ORD'], row['FX'], self.Ratio, self.FX), axis=1)

#         # Return Indicator
#         return indicator[self.name]

#     @property
#     def requiredSecurities(self):
#         """
#         Returns dictionary of functions required to load data for indicator
#         """
#         return np.asarray([self.ADR, self.ORD, self.FX])


# def getIndicatorList(ticker='all'):
#     """
#     Returns a sorted list of Indicator names
#     """
#     indicator_names = np.asarray([])
#     if ticker == 'all':
#         clsmembers = inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__)
#         for name, obj in clsmembers:
#             indicator_names = np.append(indicator_names, name)

#     return np.sort(indicator_names)
