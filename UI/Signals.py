from threading import Thread

import numpy as np
import pandas as pd
import zmq
from functools import partial

from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.clock import Clock

from kivy.properties import StringProperty, DictProperty
from Strategy import getStrategyList, loadStrategy
from ADR import getUniverse, getORD, getADRFX, getFutures, getADRRatio, calcADRPremium

from Data import getMarketCloseCodeByTicker

import pdb


class Signals(GridLayout):

    def generateSignals(self):
        # Thread(target=self.update).start()
        context = zmq.Context()
        sock = context.socket(zmq.REQ)
        sock.connect('tcp://localhost:8080')

        update_func = partial(self.update, sock)
        Clock.schedule_interval(update_func, 1)
        self.update(sock)

    def update(self, sock, *largs):

        ADR = self.signal_ticker
        sock.send(ADR)

        message = sock.recv()

        message = message.split(',')

        adr_price = message[0]
        self.ids['adr_price'].text = adr_price

        if len(message) == 2:

            ord_price = message[1]
            self.ids['ord_price'].text = ord_price

        if len(message) > 2:

            ord_close = message[2]
            self.ids['ord_close'].text = ord_close

            adr_premium = message[3]
            self.ids['adr_premium'].text = adr_premium

            futures_ret = message[4]
            self.ids['fut_ret'].text = futures_ret

            indicator = message[5]
            self.ids['indicator'].text = indicator

        print message

    def update_universe(self, strategy):
        strat = loadStrategy(strategy)
        if strat['type'] == 'ADR/Local':
            self.ids['ticker'].values = getUniverse()

    def getStrategyList(self):
        return getStrategyList()


class SignalsApp(App):

    def build(self):
        return Signals()


if __name__ == '__main__':
    SignalsApp().run()
