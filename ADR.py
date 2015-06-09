import pandas as pd
from re import findall
import pdb


def getWorstCaseTurn(ticker, side, price=None):
    """
    Get Worst case turn cost for ADR
    """
    reference = pd.DataFrame.from_csv('ADR_test.csv')
    # If long ADRs
    if side == 'Buy':
        cost = str(reference.ix[ticker].Turn_Long)
    # If short ADRs
    if side == 'Short':
        cost = str(reference.ix[ticker].Turn_Short)
    # If bps, calculate cost
    if 'bps' in cost:
        cost = float(findall('\d+', cost)[0]) / 10000 * price
    return float(cost)


def getADRTable():
    """
    Load DataFrame containing ADR universe and information
    """
    return pd.DataFrame.from_csv('ADR_test.csv')


def getUniverse(data=None):
    """
    Load DataFrame containing list of ADR tickers
    """
    if data is None:
        reference = getADRTable()
    else:
        reference = data
    return reference.index.values


def getADRFX(ticker, data=None):
    """
    Returns ADR ratio given ADR ticker
    """
    if data is None:
        reference = getADRTable()
    else:
        reference = data
    return reference.ix[ticker].FX


def getADRRatio(ticker, data=None):
    """
    Returns ADR ratio given ADR ticker
    """
    if data is None:
        reference = getADRTable()
    else:
        reference = data
    return reference.ix[ticker].Ratio


def getORD(ticker, data=None):
    """
    Returns ORD ticker given ADR ticker
    """
    if data is None:
        reference = getADRTable()
    else:
        reference = data
    return reference.ix[ticker].ORD


def calcADREquiv(ORD_price, FX_price, ratio, FX):
    """
    Calculate ADR Equivalent Price for an ORD security
    """
    # IF GBP/ZAR/ILS need to divide price by 100 first
    if FX.split(' ')[0] in ['GBP', 'ZAR', 'ILS']:
        return (ORD_price / 100.) * ratio * FX_price

    # IF DKK/CHF/NOK/SEK divide FX
    elif FX.split(' ')[0] in ['DKK', 'CHF', 'NOK', 'SEK', 'JPY']:
        return ORD_price * ratio / FX_price

    # All other cases
    else:
        return ORD_price * ratio * FX_price


def calcADRPremium(ADR_price, ORD_price, FX_price, ratio, FX, method='pct'):
    """
    Calculate ADR vs Ord Premium (pct or abs)
    """
    # Calculate ADR equivalent price for ORD
    ADR_equiv_price = calcADREquiv(ORD_price, FX_price, ratio, FX)

    # Calculate absolute premium
    abs_premium = ADR_price - ADR_equiv_price

    # If method absolute, return absolute
    if method == 'abs': return abs_premium

    # IF method pct, calc pct and return
    if method == 'pct': return (abs_premium / ADR_equiv_price) * 100
