from kivy.app import App
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.dropdown import DropDown
from kivy.uix.image import Image
from kivy.uix.textinput import TextInput
from kivy.uix.spinner import Spinner
from kivy.uix.slider import Slider
from kivy.uix.checkbox import CheckBox
from Data import updateTickData, getPrices
from Backtest import backtest, optimize
from Strategy import ADR_basic
from ADR import getUniverse, getADRFX, getORD, getWorstCaseTurn, getADRRatio, getFutures, calcADREquiv, calcADRPremium
from Indicator import getIndicatorList
import numpy as np
import pandas as pd
import pylab as plt
import pdb


class ParamWindow(GridLayout):
    """
    Box to Hold Parameter

    Header - param header
    Val = either str or array of str depending on whether fixed or not
    """
    def __init__(self, header, val, fixed=False, **kwargs):
        super(ParamWindow, self).__init__(**kwargs)

        self.rows = 2
        self.cols = 1

        # Header
        self.header = Label(text=header, size_hint=(1, .2))

        self.add_widget(self.header)

        # If fixed, create fixedlabel
        if fixed:
            self.val_window = FixedLabel(val, size_hint=(1, .8))

        # Else, create CustomSpinner
        else:
            self.val_window = CustomSpinner(val, size_hint=(1, .8))

        self.add_widget(self.val_window)

    def getValue(self):
        return self.val_window.getValue()

    def setValue(self, value):
        self.val_window.setValue(value)


class CustomSpinner(Spinner):
    """
    Dropdown to select value from a set with default value
    """
    def __init__(self, values, **kwargs):
        super(CustomSpinner, self).__init__(**kwargs)

        # Convert values to string
        self.values = [str(x) for x in values]

        # Set default to first value
        self.text = self.values[0]
        self.size_hint = (None, None)
        self.size = (100, 44)

    def getValue(self):
        return self.text

    def setValue(self, value):
        self.text = str(value)


class FixedLabel(Label):
    """
    Label for fixed parameter value
    """
    def __init__(self, text, **kwargs):
        super(FixedLabel, self).__init__(**kwargs)

        self.text = text
        self.bold = True
        self.size = (100, 44)
        self.size_hint = (None, None)

    def getValue(self):
        return self.text

    def setValue(self, value):
        self.text = str(value)


class Consequent(GridLayout):
    """
    Box to Generate Consequent for expression
    """
    def __init__(self, expression, fix_security=None, fix_action=None, fix_params_start=None, fix_params_end=None, **kwargs):
        super(Consequent, self).__init__(**kwargs)

        self.rows = 1
        self.cols = 5
        self.spacing = [10, 0]

        # If action fixed, set to fixed Label
        if fix_action is not None:
            action = fix_action
            fixed = True

        # If not, set to spinner
        else:
            action = np.asarray(['Buy', 'Sell', 'Short'])
            fixed = False

        self.action = ParamWindow('Action:', action, fixed=fixed)
        self.add_widget(self.action)

        # If security is specified,  set to fixed label
        if fix_security is not None:
            security = fix_security
            fixed = True

        # If not, set to spinner
        else:
            security = np.asarray(['ADR', 'ORD'])
            fixed = False

        self.security = ParamWindow('Security:', security, fixed=fixed)
        self.add_widget(self.security)

        # Algo
        algos = np.asarray(['VWAP'])
        self.algo = ParamWindow('Algo:', algos)
        self.add_widget(self.algo)

        # If algo param start specified, set to fixed label
        if fix_params_start is not None:
            start = fix_params_start
            fixed = True

        # If not, set to spinner
        else:
            start = np.asarray(['Now', 'MktOpen'])
            fixed = False

        self.start = ParamWindow('Start:', start, fixed=fixed)
        self.add_widget(self.start)

        # If algo end param specified, set to fixed label
        if fix_params_end is not None:
            end = fix_params_end
            fixed = True

        # If not, set to spinner
        else:
            end = np.asarray(['MktClose', '+5', '+10', '+15', '+30', '+60', '+90', '+120'])
            fixed = False

        self.end = ParamWindow('End:', end, fixed=fixed)
        self.add_widget(self.end)


class Antecedent(GridLayout):
    """
    Box to Generate Antecedent for expression
    """
    def __init__(self, expression, side, fix_indicator=None, **kwargs):
        super(Antecedent, self).__init__(**kwargs)

        self.rows = 1
        self.cols = 5
        self.spacing = [10, 0]

        # Set time of day
        # If futures condition, tie to premium time of day
        if fix_indicator == 'Futures_Return':
            param_options = expression.premium_antecedent.time_of_day.getValue()
            self.time_of_day = ParamWindow('At time:', param_options, fixed=True)
            dropdown = expression.premium_antecedent.time_of_day.val_window._dropdown
            dropdown.bind(on_select=lambda instance, x: self.time_of_day.setValue(x))

        else:
            param_options = np.asarray(['13:00', '13:30', '14:00', '14:30', '15:00', '15:30'])
            self.time_of_day = ParamWindow('At time:', param_options)

        self.add_widget(self.time_of_day)

        # If indicator specified, set to fix
        if fix_indicator is not None:
            indicator = fix_indicator
            fixed = True

        # Otherwise get indicator list
        else:
            indicator = getIndicatorList()
            fixed = False
        self.indicator = ParamWindow('Indicator:', indicator, fixed=fixed)
        self.add_widget(self.indicator)

        # Logic
        if side == 'long':
            if fix_indicator == 'Futures_Return':
                logic = '>'
            else:
                logic = '<'
        else:
            if fix_indicator == 'Futures_Return':
                logic = '<'
            else:
                logic = '>'
        self.logic = ParamWindow('Logic', logic, fixed=True)
        self.add_widget(self.logic)

        # Value input
        if side == 'long':
            values = np.round(np.arange(-0.02, 0.002, .002, dtype=float), decimals=5)[::-1]
        else:
            values = np.round(np.arange(0, .022, .002, dtype=float), decimals=5)
        self.value = ParamWindow('Value:', values)
        self.add_widget(self.value)


class Expression(GridLayout):
    """
    Box to Generate Rule For Signal
    """
    def __init__(self, side, **kwargs):
        super(Expression, self).__init__(**kwargs)

        self.rows = 8
        self.cols = 1
        self.spacing = [0, -30]
        self.padding = 2

        # Antecedents Header
        self.add_widget(Label(text='IF'))

        # First Antecedent
        self.premium_antecedent = Antecedent(self, side, fix_indicator='ADR_Premium')
        self.add_widget(self.premium_antecedent)

        # Antecedents Header
        self.add_widget(Label(text='AND'))

        # Second Antecedent
        self.futures_antecedent = Antecedent(self, side, fix_indicator='Futures_Return')
        self.add_widget(self.futures_antecedent)

        if side == 'long':
            ADR_action = 'Buy'
            ORD_action = 'Short'
        else:
            ADR_action = 'Short'
            ORD_action = 'Buy'

        # ADR Consequent Header
        self.add_widget(Label(text='IMMEDIATELY'))

        # ADR Consequent
        self.add_widget(Consequent(self, fix_security='ADR', fix_action=ADR_action, fix_params_start='Now', fix_params_end='MktClose'))

        # ORD Consequent Header
        self.add_widget(Label(text='AND AT NEXT LOCAL OPEN'))

        # ORD Consequent
        self.add_widget(Consequent(self, fix_security='ORD', fix_action=ORD_action, fix_params_start='MktOpen'))


class Expressions(GridLayout):
    """
    Displays one expression with option of extending
    """
    def __init__(self, **kwargs):
        super(Expressions, self).__init__(**kwargs)

        self.cols = 2
        self.rows = 1

        self.spacing = [50, 0]

        # Add two Expressions (Premium / Discount)
        self.add_widget(Expression('long'))
        self.add_widget(Expression('short'))

    def addExpression(self, button):
        """
        Add another expression
        """
        # Increase rows
        self.rows += 1

        # Get children, convert to np array
        children = np.asarray(self.children)

        # Get position of last Antecedent
        for i, x in enumerate(children):
            if type(x) == Expression:
                ix = i

        # Add widget
        self.add_widget(Expression(), index=ix)


class ADRInfo(GridLayout):
    """
    Displays information about ADR
    """
    def __init__(self, ADR_select, **kwargs):
        super(ADRInfo, self).__init__(**kwargs)

        self.rows = 6
        self.cols = 2

        self.ADR = ADR_select.text

        self.ORD = Label(text='')
        self.FX = Label(text='')
        self.Ratio = Label(text='')
        self.MaxTurnLong = Label(text='')
        self.MaxTurnShort = Label(text='')
        self.Futures = Label(text='')

        self.update(self.ADR + ' US Equity')

        # Show ORD
        self.add_widget(Label(text='ORD:'))
        self.add_widget(self.ORD)

        # Show FX
        self.add_widget(Label(text='FX:'))
        self.add_widget(self.FX)

        # Show Ratio
        self.add_widget(Label(text='Ratio:'))
        self.add_widget(self.Ratio)

        # Show Worst Case Turn Long
        self.add_widget(Label(text='Max. Turn (Long):'))
        self.add_widget(self.MaxTurnLong)

        # Show Worst Case Turn Short
        self.add_widget(Label(text='Max. Turn (Short):'))
        self.add_widget(self.MaxTurnShort)

        # Get Futures
        self.add_widget(Label(text='Futures:'))
        self.add_widget(self.Futures)

    def update(self, ADR):
        """
        Update field values
        """
        self.ADR = ADR
        self.ORD.text = ' '.join(getORD(self.ADR).split(' ')[:2])
        self.FX.text = ' '.join(getADRFX(self.ADR).split(' ')[:1])
        self.Ratio.text = str(getADRRatio(self.ADR))
        self.MaxTurnLong.text = str(getWorstCaseTurn(self.ADR, 'Buy'))
        self.MaxTurnShort.text = str(getWorstCaseTurn(self.ADR,'Short'))
        self.Futures.text = ' '.join(getFutures(self.ADR).split(' ')[:1])


class ADRSelect(GridLayout):
    """
    Contains ADR Selection and Info
    """
    def __init__(self, **kwargs):
        super(ADRSelect, self).__init__(**kwargs)

        self.rows = 1
        self.cols = 2
        self.size_hint = (1, .5)

        # SecuritySelector
        ADRs = [x.split(' ')[0] for x in getUniverse()]
        self.ADR_select = CustomSpinner(ADRs)
        self.ADR_info = ADRInfo(self.ADR_select)
        dropdown = self.ADR_select._dropdown
        dropdown.bind(on_select=lambda instance, x: self.ADR_info.update(x + ' US Equity'))

        # ADR info
        self.add_widget(self.ADR_select)
        self.add_widget(self.ADR_info)


class DaysSlider(GridLayout):
    """
    Slider to get number of days going back
    """
    def __init__(self, **kwargs):
        super(DaysSlider, self).__init__(**kwargs)

        self.rows = 3
        self.cols = 1

        self.add_widget(Label(text='Number of days to backtest:'))

        default_days = 50
        self.num_days = Label(text=str(default_days))
        self.slider = Slider(min=1, max=100, step=1, value=default_days)
        def OnSliderValueChange(instance, value):
            self.num_days.text = str(int(value))

        self.slider.bind(value=OnSliderValueChange)
        self.add_widget(self.num_days)
        self.add_widget(self.slider)


class CustomCheckBox(BoxLayout):
    """
    Custom checkbox with text before
    """
    def __init__(self, text, active=False, **kwargs):
        super(CustomCheckBox, self).__init__(**kwargs)

        self.orientation = 'vertical'

        description = Label(text=text)
        checkbox = CheckBox(active=active)

        self.add_widget(description)
        self.add_widget(checkbox)


class TestOptions(GridLayout):
    """
    Options for Backtest
    """
    def __init__(self, **kwargs):
        super(TestOptions, self).__init__(**kwargs)

        self.rows = 1
        self.cols = 2
        self.size_hint = (1, .2)

        # Automatically do FX
        self.fx_checkbox = CustomCheckBox('Automatic FX', active=True)
        self.add_widget(self.fx_checkbox)

        # Number of days to test
        self.days_slider = DaysSlider()
        self.add_widget(self.days_slider)

        # Minimum ORD liquidity ($$mm)
        # min_liquidity_box = GridLayout(rows=1, cols=2)
        # min_liquidity_box.add_widget(Label(text='Min. Daily ORD liquidity (USD)'))
        # min_liquidty = np.asarray([10, 20, 30, 50, 60, 70, 80, 90, 100])
        # self.min_liquidity = GridLayout(min_liquidit


class ParameterFrame(GridLayout):
    """
    Box that stores parameter frame for single backtest
    """
    def __init__(self, main, **kwargs):
        super(ParameterFrame, self).__init__(**kwargs)

        self.rows = 4
        self.cols = 1
        self.spacing = [0, 30]

        # Main windows
        self.main = main

        # Add ADR Select and Info
        self.ADR_select = ADRSelect()
        self.add_widget(self.ADR_select)

        # Add Options
        self.test_options = TestOptions()
        self.add_widget(self.test_options)

        # Add expressions
        self.expressions = Expressions()
        self.add_widget(self.expressions)

        # Add backtest Button
        backtest = Button(text='Run Backtest', size_hint_y=.1)
        backtest.bind(on_press=self.doBacktest)
        self.add_widget(backtest)

        # Backtest results
        self.current_strategy = None

    def doBacktest(self, button):
        """
        Collect input information and send to backtest
        """
        # Get ADR
        ADR = self.ADR_select.ADR_select.text + ' US Equity'

        # Get number of Days to backtest
        num_days = int(self.test_options.days_slider.num_days.text)

        # Get Automatic FX
        auto_fx = True

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

        result, data, speed = backtest(self.current_strategy, days=num_days)

        self.main.results.update(result, data, speed)


class ResultsSummary(GridLayout):
    """
    Shows summary of backtest results
    """
    def __init__(self, results, data, speed, **kwargs):
        super(ResultsSummary, self).__init__(**kwargs)

        self.cols = 2
        self.rows = 8

        # Start date
        start_date = data.values()[0].index.min().date().strftime('%m/%d/%y')
        end_date = data.values()[1].index.max().date().strftime('%m/%d/%y')
        self.add_widget(Label(text='Date Range: '))
        self.add_widget(Label(text=start_date + ' - ' + end_date))

        # Long ADR trades
        long_trades = len(results[results['Buy/Sell ADR'] == 'Buy'])
        self.add_widget(Label(text='Long ADR trades: '))
        self.add_widget(Label(text=str(long_trades)))

        # Short ADR trades
        short_trades = len(results[results['Buy/Sell ADR'] == 'Short'])
        self.add_widget(Label(text='Short ADR trades: '))
        self.add_widget(Label(text=str(short_trades)))

        # Max. Net Profit
        max_net_profit = results['Net Profit (bps)'].max()
        self.add_widget(Label(text='Max. Net Profit: '))
        self.add_widget(Label(text=str(round(max_net_profit, 2)) + ' (bps)'))

        # Min. Net Profit
        min_net_profit = results['Net Profit (bps)'].min()
        self.add_widget(Label(text='Min. Net Profit: '))
        self.add_widget(Label(text=str(round(min_net_profit, 2)) + ' (bps)'))

        # Avg. Net Profit
        avg_net_profit_bps = results['Net Profit (bps)'].mean()
        self.add_widget(Label(text='Avg. Net Profit: '))
        self.add_widget(Label(text=str(round(avg_net_profit_bps, 2)) + ' (bps)'))

        # Risk Adj. Net Profit
        risk_adj_net_profit_bps = results['Net Profit (bps)'].mean() / results['Net Profit (bps)'].std()
        self.add_widget(Label(text='Risk. Adj. Avg. Net Profit: '))
        self.add_widget(Label(text=str(round(risk_adj_net_profit_bps, 2)) + ' (bps)'))

        # Speed
        self.add_widget(Label(text='Speed: '))
        self.add_widget(Label(text=str(speed) + ' (s)'))


class ResultsWindow(GridLayout):
    """
    Window to display backtest summary and histogram
    """
    def __init__(self, main, **kwargs):
        super(ResultsWindow, self).__init__(**kwargs)

        self.main = main
        self.rows = 1
        self.cols = 2

    def update(self, results, data, speed):
        """
        Update window with data
        """
        # If no trades occurred
        if results is None:
            self.add_widget(Label(text='NO TRADES'))

        # Clear widgets
        self.clear_widgets()

        # TOP ROW
        left_col = GridLayout(cols=1, rows=2)

        # Add summary
        left_col.add_widget(ResultsSummary(results, data, speed))

        # Add Return Histogram
        fig, plt1 = plt.subplots(1, 1)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.tick_params(axis='x', colors='white')
        numbins = 10
        points = results['Net Profit (bps)'].values
        plt1.hist(points, bins=numbins, normed=False, color='white', alpha=0.4, histtype='bar')
        plt1.plot([0, 0], plt1.get_ylim(), 'k--', lw=1)
        risk_adj_profit = results['Net Profit (bps)'].mean() / results['Net Profit (bps)'].std()
        plt1.plot([risk_adj_profit, risk_adj_profit], plt1.get_ylim(), 'r--', lw=1)
        hist_loc = 'figs/hist.png'
        plt.savefig(hist_loc, bbox_inches='tight', transparent=True)

        # Add histogram plot
        left_col.add_widget(Image(source=hist_loc))

        # BOTTOM ROW
        right_col = GridLayout(cols=1, rows=2)

        # Add graph of ADR price and ADR equivalent price
        data.keys()
        for key in data.keys():
            if 'US' in key: ADR = key
        ORD = getORD(ADR)
        FX = getADRFX(ADR)
        Ratio = getADRRatio(ADR)

        start = data.values()[0].index.min()
        end = data.values()[0].index.max()
        dt_index = pd.bdate_range(start, end, freq='1min', normalize=False)

        prices = pd.DataFrame(index=dt_index)
        x = dt_index.to_pydatetime()

        prices['ADR'] = getPrices(ADR, dt_index, data=data[ADR], VWAP_only=True)
        prices['ORD'] = getPrices(ORD, dt_index, data=data[ORD], VWAP_only=True)
        prices['FX'] = getPrices(FX, dt_index, data=data[FX])
        prices['ADR Equiv'] = prices.apply(lambda x: calcADREquiv(x.ORD, x.FX, Ratio, FX), axis=1)
        prices['Premium'] = prices.apply(lambda x: calcADRPremium(x.ADR, x.ORD, x.FX, Ratio, FX), axis=1)

        # Create plot for prices
        fig, plt1 = plt.subplots(1, 1)
        plt1.plot(x, prices.ADR.values, color='white', lw=.00)
        plt1.tick_params(axis='y', colors='white')
        plt1.plot(x, prices['ADR Equiv'], color='white', lw=.0)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.fill_between(x, prices.ADR.values, prices['ADR Equiv'], where=(prices.ADR.values > prices['ADR Equiv']), color='green', alpha=0.5)
        plt1.fill_between(x, prices.ADR.values, prices['ADR Equiv'], where=(prices.ADR.values < prices['ADR Equiv']), color='red', alpha=0.5)
        prices_loc = 'figs/prices.png'
        plt.savefig(prices_loc, bbox_inches='tight', transparent=True)

        # Add prices plot
        right_col.add_widget(Image(source=prices_loc))

        # Create plot for premium
        fig, plt1 = plt.subplots(1, 1)
        plt1.plot(x, prices.Premium.values, color='white', lw=0.0)
        plt1.spines['bottom'].set_color('white')
        plt1.spines['left'].set_color('white')
        plt1.tick_params(axis='y', colors='white')
        plt1.fill_between(x, prices.Premium.values, 0, where=(prices.Premium.values < 0), color='red', alpha=0.5)
        plt1.fill_between(x, prices.Premium.values, 0, where=(prices.Premium.values > 0), color='green', alpha=0.5)
        premium_loc = 'figs/premium.png'
        plt.savefig(premium_loc, bbox_inches='tight', transparent=True)

        # Add prices plot
        right_col.add_widget(Image(source=premium_loc))

        self.add_widget(left_col)
        self.add_widget(right_col)


class SingleBackTestingFrame(GridLayout):
    """
    Single backtest frame
    """
    def __init__(self, **kwargs):
        super(SingleBackTestingFrame, self).__init__(**kwargs)

        self.cols = 2
        self.rows = 1

        # Add parameter window
        self.params = ParameterFrame(self)
        self.add_widget(self.params)

        # Add results window
        self.results = ResultsWindow(self.params)
        self.add_widget(self.results)


class TestApp(App):
    def build(self):

        frame = SingleBackTestingFrame()

        return frame

if __name__ == '__main__':
    TestApp().run()
