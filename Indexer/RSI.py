# -*- coding: utf-8 -*-
"""
LC := REF(CLOSE,1);
RSI1:SMA(MAX(CLOSE-LC,0),N1,1)/SMA(ABS(CLOSE-LC),N1,1)*100;
RSI2:SMA(MAX(CLOSE-LC,0),N2,1)/SMA(ABS(CLOSE-LC),N2,1)*100;
"""
import pandas as pd
import talib
import numpy as np

def rsi(close, n):
    # 计算rsi
    close_array = np.array(close.values, dtype='float')
    #lc1 = close - close.shift(1)
    #lc1_cmp0 = (lc1 + lc1.abs()) / 2
    #return lc1_cmp0.rolling(n).mean() / lc1.abs().rolling(n).mean() * 100
    return talib.RSI(close_array, n)


if __name__ == "__main__":
    import DATA_CONSTANTS as DC
    #rawdata = pd.read_csv('..\\test.csv')
    rawdata = DC.getBarBySymbol("SHFE.RB", "RB1810", 3600)
    rawdata['rsi'] = rsi(rawdata['close'], 5)
    rawdata.to_csv('rsi.csv')
    pass
