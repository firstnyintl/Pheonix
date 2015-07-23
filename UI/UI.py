import multiprocessing
from subprocess import Popen

from kivy.app import App
from kivy.lang import Builder
from kivy.uix.button import Button
from kivy.uix.gridlayout import GridLayout


Builder.load_string("""

<Overview>:
    rows: 2
    cols: 1
    Button:
        text: 'Create Strategy'
        on_release: root.createStrategy()
    Button:
        text: 'Generate Signals'
        on_release: root.generateSignals()
""")


class Overview(GridLayout):

    def createStrategy(self, *btn_args):
        Popen('ipython CreateStrategy.py')

    def generateSignals(self, *btn_args):
        Popen('ipython Signals.py')


class UIApp(App):

    def build(self):
        return Overview()


if __name__ == '__main__':
    UIApp().run()
