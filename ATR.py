# -*- coding: utf-8 -*-
'''
公式：
TR : MAX(MAX((HIGH-LOW),ABS(REF(CLOSE,1)-HIGH)),ABS(REF(CLOSE,1)-LOW));
ATR : MA(TR,N);
原理：
（1） A=最高价-最低价
      B=（前一收盘价-最高价）的绝对值
      C=A与B两者较大者
      D=（前一收盘价-最低价）的绝对值
      TR=C与D两者较大者

（2）ATR=TR在N个周期的简单移动平均
'''
import pandas as pd
import numpy as np

def ATR(high,low,close,N=26):
    closeshift1 = close.shift(1).fillna(0)
    c = high - low
    d = np.abs(high - closeshift1)
    df1 = pd.DataFrame({'c': c, 'd': d})
    df1['b'] = np.abs(low - closeshift1)
    df1['TR'] = df1.max(axis=1)
    df1['ATR']=df1['TR'].rolling(window=N).mean()
    return df1['TR'],df1['ATR']

if __name__ == '__main__':
    N=26
    df=pd.read_csv('test.csv')
    df['TR'],df['ATR']=ATR(df['high'],df['low'],df['close'],N)
    df.to_csv('ATR.csv')
