import os
from datetime import time
import msgpack

import pandas as pd
import pylab as plt

from kivy.app import App

from kivy.uix.gridlayout import GridLayout
from kivy.uix.floatlayout import FloatLayout

from kivy.uix.label import Label
from kivy.uix.popup import Popup

from kivy.uix.settings import SettingsWithSidebar
from kivy.config import ConfigParser
from settingsjson import adr_local

from kivy.properties import NumericProperty, ObjectProperty, DictProperty, StringProperty, ListProperty

import Data
from Backtest import backtest
from Strategy import ADR_basic, getStrategyDir
from ADR import calcADREquiv, calcADRPremium, getORD, getFutures
import Indicator
import EMSX

import pdb


class BrokerField(GridLayout):

    security = StringProperty()
    broker = StringProperty()

    def updateBrokers(self, security):
        if security == 'ADR':
            self.security = self.adr
        elif security == 'ORD':
            self.security = getORD(self.adr)
        elif security == 'ORD Futures':
            self.security = getFutures(self.adr)

        brokers = EMSX.getBrokers(self.security)

        self.ids['brokers'].values = brokers

    def updateBrokerStrategies(self, broker):

        broker_strategies = EMSX.getBrokerStrategies(self.security, broker)

        self.parent.parent.ids['strategy'].values = broker_strategies


class TimeParamInputField(FloatLayout):

    def getValueAsString(self):
        return str(self.ids['hour'].text) + ':' + str(self.ids['minute'].text)


class IndicatorParamField(GridLayout):

    def __init__(self, name, typ, **kwargs):
        super(IndicatorParamField, self).__init__(**kwargs)
        self.rows = 2
        self.cols = 1
        self.add_widget(Label(text=name))
        if typ == time:
            self.add_widget(TimeParamInputField())

    def getValueAsString(self):
        return self.children[0].getValueAsString()


class IndicatorParamsField(GridLayout):
    def buildParamFields(self):
        self.clear_widgets()
        params = self.indicator.params
        for param in params:
            self.add_widget(IndicatorParamField(param['name'], param['type']))

    def getIndicator(self, name):
        return getattr(Indicator, name)

    def getParamsAsString(self):
        params_string = '('
        j = 0
        for child in self.children:
            if j > 0:
                params_string += ','
            params_string += child.getValueAsString()
        params_string += ')'
        return params_string


class ResultsWindow(GridLayout):

    strategy = ObjectProperty()
    results = ObjectProperty()
    data = DictProperty()
    speed = NumericProperty()
    hist_loc = StringProperty('figs/hist.png')
    prices_loc = StringProperty('figs/prices.png')
    premium_loc = StringProperty('figs/premium.png')

    def __init__(self, main, **kwargs):
        super(ResultsWindow, self).__init__(**kwargs)

        self.main = main
        # self.rows = 1
        # self.cols = 2

    def draw_charts(self):

        fig, plt1 = plt.subplots(1, 1)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.tick_params(axis='x', colors='white')
        numbins = 10
        points = self.results['Net Profit (bps)'].values
        plt1.hist(points, bins=numbins, normed=False, color='white', alpha=0.4, histtype='bar')
        plt1.plot([0, 0], plt1.get_ylim(), 'k--', lw=1)
        risk_adj_profit = self.results['Net Profit (bps)'].mean() / self.results['Net Profit (bps)'].std()
        plt1.plot([risk_adj_profit, risk_adj_profit], plt1.get_ylim(), 'r--', lw=1)
        plt.savefig(self.hist_loc, bbox_inches='tight', transparent=True)

        start = self.data.values()[0].index.min()
        end = self.data.values()[0].index.max()
        dt_index = pd.bdate_range(start, end, freq='1min', normalize=False)

        prices = pd.DataFrame(index=dt_index)
        x = dt_index.to_pydatetime()

        prices['ADR'] = Data.getPrices(self.strategy.ADR, dt_index, data=self.data[self.strategy.ADR], VWAP_only=True)
        prices['ORD'] = Data.getPrices(self.strategy.ORD, dt_index, data=self.data[self.strategy.ORD], VWAP_only=True)
        prices['FX'] = Data.getPrices(self.strategy.FX, dt_index, data=self.strategy.data[self.strategy.FX])
        prices['ADR Equiv'] = prices.apply(lambda x: calcADREquiv(x.ORD, x.FX, self.strategy.Ratio, self.strategy.FX), axis=1)
        prices['Premium'] = prices.apply(lambda x: calcADRPremium(x.ADR, x.ORD, x.FX, self.strategy.Ratio, self.strategy.FX), axis=1)

        # Create plot for prices
        fig, plt1 = plt.subplots(1, 1)
        plt1.plot(x, prices.ADR.values, color='white', lw=.00)
        plt1.tick_params(axis='y', colors='white')
        plt1.plot(x, prices['ADR Equiv'], color='white', lw=.0)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.fill_between(x, prices.ADR.values, prices['ADR Equiv'], where=(prices.ADR.values > prices['ADR Equiv']), color='green', alpha=0.5)
        plt1.fill_between(x, prices.ADR.values, prices['ADR Equiv'], where=(prices.ADR.values < prices['ADR Equiv']), color='red', alpha=0.5)
        plt.savefig(self.prices_loc, bbox_inches='tight', transparent=True)

        fig, plt1 = plt.subplots(1, 1)
        plt1.plot(x, prices.Premium.values, color='white', lw=0.0)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.tick_params(axis='y', colors='white')
        plt1.fill_between(x, prices.Premium.values, 0, where=(prices.Premium.values < 0), color='red', alpha=0.5)
        plt1.fill_between(x, prices.Premium.values, 0, where=(prices.Premium.values > 0), color='green', alpha=0.5)
        plt.savefig(self.premium_loc, bbox_inches='tight', transparent=True)


class BackTestLoadout(GridLayout):

    def doBacktest(self, root, *btn_args):
        """
        Collect input information and send to backtest
        """
        # Get ADR
        pdb.set_trace()
        ADR = self.ids['adr_spinner'].text + ' US Equity'

        # Get number of Days to backtest
        num_days = int(self.ids['days_slider'].value)

        # Get Automatic FX
        auto_fx = self.ids['auto_fx'].active

        # Get all expressions
        expressions = self.expressions.children

        antecedents = pd.Series()
        consequents = pd.Series()

        for i, expression in enumerate(expressions):

            # Get antecedents
            antecedent_list = [x for x in expression.children if type(x) is Antecedent]

            # Build antecedent string 
            antecedent_string = ''

            for j, antecedent in enumerate(list(reversed(antecedent_list))):
                indicator = antecedent.indicator.getValue()
                param = antecedent.time_of_day.getValue()
                logic = antecedent.logic.getValue()
                value = antecedent.value.getValue()

                if j > 0: antecedent_string += ' & '
                antecedent_string += indicator + '(' + param + ')' + ' ' + logic + ' ' + value
                antecedents = antecedents.set_value(i, antecedent_string)

            # Build Antecedent
            consequent_list = [x for x in expression.children if type(x) is Consequent]

            # Build Consequent String
            consequent_string = ''

            for j, consequent in enumerate(list(reversed(consequent_list))):
                action = consequent.action.getValue()
                security = consequent.security.getValue()
                algos = consequent.algo.getValue()
                start = consequent.start.getValue()
                end = consequent.end.getValue()

                if j > 0: consequent_string += ' & '
                consequent_string += action + ' ' + security + ' ' + algos + '(' + start + ',' + end + ')'
                consequents = consequents.set_value(i, consequent_string)

        rules = pd.DataFrame(columns=['Antecedents', 'Consequents'])
        rules['Antecedents'] = antecedents
        rules['Consequents'] = consequents

        self.current_strategy = ADR_basic(rules, ADR)

        self.result, self.data, self.speed = backtest(self.current_strategy, days=num_days)

        # self.main.results.update(result, data, speed)


class Condition(FloatLayout):
    def getIndicatorList(self):
        return Indicator.getIndicatorList()


class SignalBox(GridLayout):

    signal = ListProperty()

    def update(self, signal):
        self.signal = signal

    def edit_signal(self):
        signal_window = self.signal[2]
        signal_window.mode = 'edit'
        popup = signal_window.parent_popup
        popup.title = 'Edit Signal'
        popup.open()

    def remove_signal(self):
        self.parent.signals.remove(self.signal)
        self.parent.remove_widget(self)


class Signals(GridLayout):

    signals = ListProperty()

    def newSignal(self):
        conditions = SignalConditions()
        conditions.main_window = self
        popup = Popup(title='New Signal', content=conditions, auto_dismiss=False)
        conditions.parent_popup = popup
        popup.open()

    def add_signal(self, signal):
        self.signals.append(signal)
        self.rows += 1
        signal_box = SignalBox()
        signal_box.update(signal)
        self.add_widget(signal_box)


class SignalConditions(FloatLayout):

    parent_popup = ObjectProperty()
    num_conditions = NumericProperty(0)
    main_window = ObjectProperty()
    mode = StringProperty()
    last_widget = ObjectProperty()

    def add_condition(self):

        conditions = self.ids['conditions']

        if self.num_conditions >= 5:
            return

        elif self.num_conditions != 0:
            ypos = self.get_new_y_pos()
            y_size = 0.05
            y_spacing = 0.03
            x_spacing = 0.4
            logic_select = Label(text='AND', pos_hint={'x': x_spacing, 'y': (ypos-y_size-y_spacing)}, size_hint=(.2, y_size))
            conditions.add_widget(logic_select)

            ypos = logic_select.pos_hint['y']

        else:
            ypos = 1

        y_size = 0.10
        y_spacing = 0.03
        x_spacing = 0.02
        new_condition = Condition(pos_hint={'x': x_spacing, 'y': (ypos-y_size-y_spacing)}, size_hint=(1-(2*x_spacing), y_size))

        conditions.add_widget(new_condition)
        self.num_conditions += 1
        self.last_widget = new_condition

    def get_new_y_pos(self):
        return self.last_widget.pos_hint['y']

    def remove_condition(self, condition):
        conditions = self.ids['conditions']

        if self.num_conditions > 1:
            if conditions.children.index(condition) == (len(conditions.children) - 1):
                ix = -1
            else:
                ix = 1
            logic_button_to_delete = conditions.children[conditions.children.index(condition)+ix]
            conditions.remove_widget(logic_button_to_delete)

        conditions.remove_widget(condition)
        self.num_conditions -= 1

        for child in conditions.children[::-1]:
            if type(child) == Condition:
                self.last_widget = child
                return

    def add_signal(self):

        name = self.ids['signal_name'].text
        children = self.ids['conditions'].children

        condition_string = ''
        j = 0
        for child in children:
            if type(child) is Condition:
                indicator = child.ids['indicator'].text
                logic = child.ids['logic'].text
                value = child.ids['value'].text

                if j > 0: condition_string += ' & '
                condition_string += indicator + ' ' + logic + ' ' + value
                j += 1

        signal = [name, condition_string, self]

        self.parent_popup.dismiss()

        if self.mode == 'edit':
            for child in self.main_window.children:
                if child.signal[2] == self:
                    child.signal = signal
                    return

        self.main_window.add_signal(signal)


class CreateStrategyWindow(FloatLayout):

    def save_strategy(self):

        strategy_name = self.ids['strategy_name'].text
        stratety_type = self.ids['strategy_type'].text
        signals = [signal[:2] for signal in self.ids['signals'].signals]
        signals_dict = {key: value for (key, value) in signals}

        strategy_dict = {'name': strategy_name,
                         'type': stratety_type,
                         'signals': signals_dict}

        packed = msgpack.packb(strategy_dict, use_bin_type=True)

        pdb.set_trace()

        strategy_dir = getStrategyDir()
        f = open(strategy_dir + strategy_name, 'w')
        f.write(packed)
        f.close()


class BackTestResultsApp(App):

    def __init__(self, strategy, days, **kwargs):
        super(BackTestResultsApp, self).__init__(**kwargs)

        self.strategy = strategy
        self.days = days
        self.result, self.data, self.speed = backtest(self.current_strategy, days=days)

    def build(self):
        return BackTestResultsApp()


class CreateStrategyApp(App):

    strategy_type = StringProperty()

    def build(self):
        self.settings_cls = SettingsWithSidebar
        self.use_kivy_settings = False
        return CreateStrategyWindow()

    def build_config(self, config):
        config.setdefaults('ADR', {
            'exclude_ord_holidays': True,
            'exclude_dividends': True,
            'exclude_earnings': True})

    def build_settings(self, settings):
        settings.add_json_panel('ADR/Local', self.config, data=adr_local)

if __name__ == '__main__':
    CreateStrategyApp().run()
