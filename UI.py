from kivy.app import App
from kivy.uix.button import Button
from kivy.uix.boxlayout import BoxLayout
from Data import updateTickData
from Backtest import backtest, optimize
from Strategy import ADR_basic


class Controls(BoxLayout):
    def __init__(self, **kwargs):
        super(Controls, self).__init__(**kwargs)
        self.orientation = 'vertical'
        btn1 = Button(text='Update Tick Data')
        btn1.bind(on_press=self.updateTickData)
        self.add_widget(btn1)
        btn2 = Button(text='Backtest SDRL')
        btn2.bind(on_press=self.backtest)
        self.add_widget(btn2)
        btn3 = Button(text='Optimize SDRL')
        btn3.bind(on_press=self.optimize)
        self.add_widget(btn3)

    def updateTickData(self, instance):
        print '\n'
        print '      START TICK DATA UPDATE       '
        print '\n'
        updateTickData()
        print '\n'
        print '      END TICK DATA UPDATE       '
        print '\n'

    def backtest(self, instance):
        print '\n'
        print '      START BACKTEST       '
        print '\n'
        backtest(ADR_basic, 'SDRL US Equity')
        print '\n'
        print '      END BACKTEST       '
        print '\n'

    def optimize(self, instance):
        print '\n'
        print '      START OPTIMIZATION       '
        print '\n'
        optimize('SDRL US Equity')
        print '\n'
        print '      END OPTIMIZATION       '
        print '\n'


class TestApp(App):
    def build(self):
        return Controls()

if __name__ == '__main__':
    TestApp().run()
