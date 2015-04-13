from datetime import datetime, timedelta
import numpy as np
import BBG
import pdb

"""
ptlist needs to be defined
"""

class broker_changes:
    def __init__(self, ticker, type, firm, analyst, PT, old_PT, date):
        self.ticker = ticker
        self.type = type
        self.firm = firm
        self.analyst = analyst
        self.PT = float(PT)
        self.old_PT = float(old_PT)
        self.date = date
        self.BBGData = BBG.getBulkAnalystRecs(self.ticker)

    def stHighLow(self):
        """
        returns string to indicate is street high or low
        """
        if self.PT > np.max(self.ptlist):
            return 'Street High'
        elif self.PT < np.min(self.ptlist):
            return 'Street Low'
        else:
            return 'Neither'

    def quartile(self):
        """
        what quartile does the new price target fall under
        """
        ptlist = self.BBGData[self.BBGData['Target Price'] > 0]['Target Price'].values
        high = np.max(ptlist)
        rng = np.ptp(ptlist)
        q = rng / 4.
        if self.PT > high - q:
            return 4
        elif self.PT > high - q * 2:
            return 3
        elif self.PT > high - q * 3:
            return 2
        else:
            return 1

    def pctChange(self):
        """
        the percent change between the new and old price target
        """
        change = self.PT - self.old_PT
        pctDif = change / self.old_PT
        result = pctDif*100
        return result

    # def changeLongTime(self):
    #     """
    #     calculated how long its been since the anayalts last peice and if that length of time is longer than 1 year, True is returned
    #     """
    #     today = self.date
    #     numberofDays = (today - 'weird ass bloomberg date').days
    #     if numberofDays > 360:
    #         return True
    #     else:
    #         return False

    def newAnalyst(self):
        """
        Returns True if there is a new analyst
        """
        return not self.BBGData[self.BBGData['Firm Name'] == self.firm].Analyst.values[0] == self.analyst.upper()

    def shortInt(self):
        """
        Boolean to define if short interest is significant
        """
        shrtInt = BBG.getSingleField(self.ticker, 'SHORT_INT_RATIO')
        if shrtInt > 5:
            return True
        else:
            return False



debug = broker_changes('MSFT', 'Upgrade', 'Goldman Sachs', 'Heather Belloni', 50, 5,'2015')

print debug.newAnalyst()
