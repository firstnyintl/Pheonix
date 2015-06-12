from datetime import datetime, time
import pandas as pd
from Data import getNonSettlementDates, getTickData
from ADR import getORD, getADRFX, getADRRatio


class ADR_basic:
    """
    BASIC ADR/LOCAL STRATEGY
    """

    def __init__(self, ADR):

        self.ADR = ADR
        self.ORD = getORD(ADR)
        self.FX = getADRFX(ADR)
        self.Ratio = getADRRatio(ADR)

        self.rules = {
            'Premium': {
                'Condition': {'indicator': 'ADR_Premium', 'logic': '>', 'value': .5},
                'Execution': [{'Action': 'Trade',
                               'Security': self.ADR,
                               'Allocation': 1.,
                               'Order_type': 'Short',
                               'Order_algo': 'VWAP',
                               'Order_algo_params': ('until_close')},

                              {'Action': 'Trade',
                               'Security': self.FX,
                               'Allocation': 1.,
                               'Order_type': 'Buy',
                               'Order_algo': 'Instant',
                               'Order_algo_params': None},

                              {'Action': 'Trade',
                               'Security': self.ORD,
                               'Allocation': 1.,
                               'Order_type': 'Buy',
                               'Order_algo': 'VWAP',
                               'Order_algo_params': ('after_open', 2.)}],
            },
            'Discount': {
                'Condition': {'indicator': 'ADR_Premium', 'logic': '<', 'value': -.5,},
                'Execution': [{'Action': 'Trade',
                               'Security': self.ADR,
                               'Allocation': 1.,
                               'Order_type': 'Buy',
                               'Order_algo': 'VWAP',
                               'Order_algo_params': ('until_close')},

                              {'Action': 'Trade',
                               'Security': self.FX,
                               'Allocation': 1.,
                               'Order_type': 'Sell',
                               'Order_algo': 'Instant',
                               'Order_algo_params': None},

                              {'Action': 'Trade',
                               'Security': self.ORD,
                               'Allocation': 1.,
                               'Order_type': 'Short',
                               'Order_algo': 'VWAP',
                               'Order_algo_params': ('after_open', 2.)}]
            }
        }

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
        for security in [self.ADR, self.ORD, self.FX]:
            securityData = getTickData(security, date_range.date.min(), date_range.date.max() + pd.tseries.offsets.BDay())
            data_dict[security] = securityData
        return data_dict

    @property
    def indicatorNames(self):
        """
        Return list of indicator names used in strategy
        """
        return self.signalRules['indicator'].unique()

    @property
    def signalRules(self):
        """
        Return DataFrame of signal rules
        """
        return pd.DataFrame(pd.DataFrame(self.rules).T.Condition.values.tolist(), index=pd.DataFrame(self.rules).T.index)

    @property
    def executionRules(self):
        """
        Return DataFrame of execution rules
        """
        return pd.DataFrame(pd.DataFrame(self.rules).T.Execution.values.tolist(), index=pd.DataFrame(self.rules).T.index)
