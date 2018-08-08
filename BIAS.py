# -*- coding: utf-8 -*-
"""
BIAS1 : (CLOSE-MA(CLOSE,L1))/MA(CLOSE,L1)*100;
BIAS2 : (CLOSE-MA(CLOSE,L2))/MA(CLOSE,L2)*100;
BIAS3 : (CLOSE-MA(CLOSE,L3))/MA(CLOSE,L3)*100;
"""
import pandas as pd


def bias(close, n):
    ma = close.rolling(n).mean()
    bi = (close - ma) / ma * 100
    return bi


if __name__ == '__main__':
    import DATA_CONSTANTS as DC
    N = 10
    testdata = DC.getBarBySymbol('SHFE.RB', 'RB1810', 3600)
    testdata['bias'] = bias(testdata['close'], 6)
    testdata.to_csv('bias.csv')
    pass
