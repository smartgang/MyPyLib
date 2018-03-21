# -*- coding: utf-8 -*-
'''
MID:MA(CLOSE,N);
TMP2:=STD(CLOSE,M);
TOP:MID+P*TMP2;
BOTTOM:MID-P*TMP2;

    收盘价向上突破下限LOWER，为买入时机
    收盘价向下突破上限UPPER，为卖出时机

参数： N  天数，在计算布林带时用，一般26天
       P　一般为2，用于调整下限的值
'''
import pandas as pd


def BOLL(close,N=26,M=26,P=2):
    mid = close.rolling(N).mean()
    mid.fillna(0, inplace=True)
    tmp2=close.rolling(M).std()
    TOP=mid+P*tmp2
    BOTTOM=mid-P*tmp2
    return mid,TOP,BOTTOM

if __name__ == '__main__':
    N=26
    M=26
    P=2
    df=pd.read_csv('test.csv')
    df['Boll_Mid'],df['Boll_Top'],df['Boll_Bottom']=BOLL(df['close'],N,M,P)
    df.to_csv('BOLL.csv')
