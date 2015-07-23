import os
from datetime import datetime, time
import pandas as pd
import numpy as np
import msgpack
from Data import getNonSettlementDates, getTickData
from ADR import getORD, getADRFX, getADRRatio, getFutures, getUniverse
import pdb


def getStrategyDir():
    return 'E:/dev/Pheonix/Strategies/'


def getStrategyList():
    return np.asarray(os.listdir(getStrategyDir()))


def loadStrategy(name):
    with open(getStrategyDir() + name) as f:
        return msgpack.unpackb(f.read())


class ADR_basic:
    """
    BASIC ADR/LOCAL STRATEGY
    """

    def __init__(self, rules, ADR):

        self.ADR = ADR
        self.ORD = getORD(ADR)
        self.FX = getADRFX(ADR)
        self.Ratio = getADRRatio(ADR)
        self.Futures = getFutures(ADR)

        self.rules = rules

    def processSignals(self, signals):
        """
        Take signals generated in backtest and make necessary changes

        -- Drop signals where ORD holiday is day after singals
        -- Drop signals yesterday (assume no tick data today)
        """
        # Drop signals where ORD holiday is day after signal
        trade_offset = 1
        holidays = getNonSettlementDates(self.ORD, signals.index.date.min(), signals.index.date.max())
        holidays = holidays - (trade_offset * pd.tseries.offsets.BDay())
        signals = signals.drop(signals.index[[signals.index.date.tolist().index(x.to_datetime().date()) for x in holidays if x.to_datetime().date() in signals.index.date]])

        # Drop signals yesterday (no tick data today)
        today = (datetime.today() - pd.tseries.offsets.BDay()).to_datetime().date()
        if today in signals.index.date.tolist():
            signals = signals.drop(signals.index[signals.index.date.tolist().index(today)])

        return signals

    def loadTickData(self, date_range):
        """
        Return dictionary of tick data required for strategy
        """
        data_dict = {}
        for security in [self.ADR, self.ORD, self.FX, self.Futures]:
            securityData = getTickData(security, date_range.date.min(), date_range.date.max() + pd.tseries.offsets.BDay())
            data_dict[security] = securityData
        return data_dict

    @property
    def indicators(self):
        """
        Return list of indicators requried to build for strategy. String format 'Indicator(param)'
        """
        rules = self.signalRules
        idx = pd.IndexSlice
        return np.unique(rules.loc[:, idx[:, ['indicator']]].values)


    @property
    def signalRules(self):
        """
        Returns rules to generate signals with
        """
        rules = self.rules.Antecedents.str.split(' ', expand=True).drop(3, axis=1).T

        antecedent_col = rules.columns.values
        df_col = np.asarray(['indicator', 'logic', 'value'])
        antecedent_col_arr = np.repeat(antecedent_col, len(df_col))
        df_col_arr = np.tile(df_col, len(antecedent_col))
        arrays = [antecedent_col_arr, df_col_arr]

        rules.index = arrays
        rules = rules.T

        return rules
