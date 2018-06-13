# -*- coding: utf-8 -*-
'''
#2017-10-26:
    增加dfCross(dfx,colum1,colum2)函数，用于计算dfx数据，中colum1和colum2列的交叉情况
    增加MA(close,MA_Short,MA_Long)函数，用于计算MA df
'''
import pandas as pd
import numpy as np
#import talib


def calMACD(closedata, short=12, long1=26, mid=9):
    '''
    计算MACD
    :param closedata:
    :param short:
    :param long1:
    :param mid:
    :return:MACD,DEA,Bar,SEMA,LEMA
    '''
    #sema = pd.ewma(closedata, span=short)
    # lema  = pd.ewma(closedata, span=long1)
    sema=closedata.ewm(span=short,adjust=False).mean()
    lema=closedata.ewm(span=long1,adjust=False).mean()
    data_dif= sema - lema
    #data_dea = pd.ewma(data_dif, span=mid)
    data_dea = data_dif.ewm(span=mid,adjust=False).mean()
    data_bar = 2 * (data_dif - data_dea)
    return data_dif,data_dea,data_bar,sema,lema

def calNewMACD(lastClose,dea,sema,lema):
    '''
    计算单个MACD值
    :param closedata: 收盘价
    :param dea:
    :param sema:
    :param lema:
    :return: MACD,DEA,BAR,SEMA,LEMA
    '''
    newSema=sema*11/13+float(lastClose)*2/13
    newLema=lema*25/27+float(lastClose)*2/27
    newMACD=newSema-newLema
    newDea=dea*8/10+newMACD*2/10
    newBar=(newMACD-newDea)*2
    return newMACD,newDea,newBar,newSema,newLema

def calKDJ(data, N=0, M=0):
    if N == 0:
        N = 9
    if M == 0:
        M = 2
    low_list = pd.rolling_min(data['low'], N)
    low_list.fillna(value=pd.expanding_min(data['low']), inplace=True)
    high_list = pd.rolling_max(data['high'], N)
    high_list.fillna(value=pd.expanding_max(data['high']), inplace=True)
    #low_list = data['row'].rolling(window=N).min()
    #low_list.fillna(value=data['low'].expanding().min(),inplace=True)
    #high_list = data['high'].rolling(window=N).max()
    #high_list.fillna(value=data['high'].expanding().max(),inplace=True)
    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    KDJ_K = pd.ewma(rsv, com=M)
    KDJ_D = pd.ewma(KDJ_K, com=M)
    KDJ_J = 3 * KDJ_K - 2 * KDJ_D
    #kdjdata.fillna(0, inplace=True)
    return low_list,high_list,rsv,KDJ_K, KDJ_D, KDJ_J

def calNewKDJ(data,kdjdata,N=9,M=2):
    '''
    计算单个KDJ的值
    1: 获取股票T日收盘价X
    2: 计算周期的未成熟随机值RSV(n)＝（Ct－Ln）/（Hn-Ln）×100，
    其中：C为当日收盘价，Ln为N日内最低价，Hn为N日内最高价，n为基期分别取5、9、19、36、45、60、73日。
    3: 计算K值，当日K值=(1-a)×前一日K值+a×当日RSV
    4: 计算D值，当日D值=(1-a)×前一日D值+a×当日K值。
    若无前一日K值与D值，则可分别用50来代替,a为平滑因子，不过目前已经约定俗成，固定为1/3。
    5: 计算J值，当日J值=3×当日K值-2×当日D值

    :return:
    '''
    datarow=data.shape[0]
    kdjrow = kdjdata.shape[0]
    closeT=data.ix[datarow-1,'close']
    Ln=min(data.ix[datarow-9:,'low'])
    Hn=max(data.ix[datarow-9:,'high'])
    if Hn==Ln:#防止出现Hn和Ln相等，导致分母为0的情况
        rsv=100
    else :
        rsv=(closeT-Ln)/(Hn-Ln)*100
    lastK=kdjdata.ix[kdjrow-1,'KDJ_K']
    lastD=kdjdata.ix[kdjrow-1,'KDJ_D']
    newK=0.66667*lastK+0.33333*rsv#不能用2/3和1/3来算，会变成int，结果变0
    newD=0.66667*lastD+0.33333*newK
    newJ=3*newK-2*newD
    kdjdata.loc[kdjrow]=[data.ix[datarow-1,'strdatetime'],data.ix[datarow-1,'utcdatetime'],Ln,Hn,rsv,newK,newD,newJ]
    #kdjdata.loc[kdjrow] = [data.ix[datarow-1, 'start_time'], rsv,Ln, Hn, newK, newD, newJ]
    pass

def calMA(data, N=5):
    #data = pd.rolling_mean(data, N)
    data = data.rolling(N).mean()
    data.fillna(0, inplace=True)
    return data

def calEMA(data,N=5):
    #ewm的adjust必须设为False，按如下公式算
    #y0 = x0
    #yt= (1−α)yt−1 + αxt,
    data = data.ewm(span=N,adjust=False).mean()
    return data

def calNewMA(data,N=5):
    '''
    data的最后N个取值计算平均值
    :param data:
    :param N:
    :return:
    '''
    return data.iloc[-N:].mean()
    #row=data.shape[0]
    #return sum(data[row-N:row])/N

def calWMA(data,weight,N=5):
    '''
    计算加权移动平均
    :param data:
    :param weight:
    :param N:
    :return:
    '''
    l=len(weight)
    d=data
    dl=d.shape[0]
    if l!=N:return
    arrWeight=np.array(weight)
    wma=pd.Series(0.0)
    for i in range(l-1,dl):
        wma[i]=sum(arrWeight*d[(i-l+1):(i+1)])
    return wma

def calNewWMA(data,weight,N=5):
    '''
    计算最后一个WMA的值
    :param data:
    :param weight:
    :param N:
    :return:
    '''
    l=len(weight)
    dl=data.shape[0]
    if l!=N:return
    arrWeight=np.array(weight)
    return sum(arrWeight*data[dl-N:dl])

def get_rsi_data(data, N=0):
    if N == 0:
        N = 24
    data['value'] = data['closeL'] - data['closeL'].shift(1)
    data.fillna(0, inplace=True)
    data['value1'] = data['value']
    data['value1'][data['value1'] < 0] = 0
    data['value2'] = data['value']
    data['value2'][data['value2'] > 0] = 0
    data['plus'] = pd.rolling_sum(data['value1'], N)
    data['minus'] = pd.rolling_sum(data['value2'], N)
    data.fillna(0, inplace=True)
    rsi = data['plus'] / (data['plus'] - data['minus']) * 100
    data.fillna(0, inplace=True)
    rsi = pd.DataFrame(rsi, columns=['rsi'])
    return rsi


def get_cci_data(data, N=0):
    if N == 0:
        N = 14
    data['tp'] = (data['highL'] + data['lowL'] + data['closeL']) / 3
    data['mac'] = pd.rolling_mean(data['tp'], N)
    data['md'] = 0
    for i in range(len(data) - 14):
        data['md'][i + 13] = data['closeL'][i:i + 13].mad()
    # data['mac']=pd.rolling_mean(data['closeL'],N)
    # data['md1']=data['mac']-data['closeL']
    # data.fillna(0,inplace=True)
    # data['md']=pd.rolling_mean(data['md1'],N)
    cci = (data['tp'] - data['mac']) / (data['md'] * 0.015)
    cci = pd.DataFrame(cci, columns=['cci'])
    return cci

def sum_N(data,n):
    tempdf=pd.DataFrame({'0':data})
    for i in np.arange(1,n):
        tempdf[str(i)]=tempdf['0'].shift(i)
    tempdf=tempdf.fillna(0)
    temp=tempdf.sum(axis=1)
    del tempdf
    return temp

def dfCross(dfx,colum1,colum2):
    dfx['true']=0
    dfx.loc[dfx[colum1] > dfx[colum2], 'true'] = 1
    dfx.loc[dfx[colum1] < dfx[colum2], 'true'] = -1

    if dfx.ix[0,'true']==0:
        dfx.ix[0,'true']=1
    #填充0值，修改为上一周期的取值
    zeroindex=dfx.loc[dfx['true']==0].index
    for zi in zeroindex:
        dfx.ix[zi,'true']=dfx.ix[zi-1,'true']

    dfx['true1'] = dfx['true'].shift(1).fillna(0)
    dfx['cross'] = 0
    dfx.loc[(dfx['true'] == 1) & (dfx['true1'] == -1), 'cross'] = 1
    dfx.loc[(dfx['true'] == -1) & (dfx['true1'] == 1), 'cross'] = -1
    true=dfx['true']
    cross=dfx['cross']
    dfx.drop('true', axis=1, inplace=True)
    dfx.drop('true1', axis=1, inplace=True)
    dfx.drop('cross', axis=1, inplace=True)
    return true,cross

#计算MA df
def MA(close,MA_Short,MA_Long):
    df_MA=pd.DataFrame({'close':close})
    df_MA['MA_Short']=calMA(df_MA['close'],MA_Short)
    df_MA['MA_Long']=calMA(df_MA['close'],MA_Long)
    df_MA['MA_True'],df_MA['MA_Cross']=dfCross(df_MA,'MA_Short','MA_Long')
    return df_MA

def EMA(close,MA_Short,MA_Long):
    df_MA=pd.DataFrame({'close':close})
    df_MA['MA_Short']=calEMA(df_MA['close'],MA_Short)
    df_MA['MA_Long']=calEMA(df_MA['close'],MA_Long)
    df_MA['MA_True'],df_MA['MA_Cross']=dfCross(df_MA,'MA_Short','MA_Long')
    return df_MA

def newMA(close,dfma,MA_Short,MA_Long):
    lastclose=close.iloc[-1]
    lasttrue=dfma.iloc[-1].MA_True
    mashort=calNewMA(close,MA_Short)
    malong=calNewMA(close,MA_Long)
    if mashort>malong:MA_True=1
    elif mashort<malong:MA_True=-1
    else:MA_True=lasttrue
    if lasttrue==-1 and MA_True==1:MA_Cross=1
    elif lasttrue==1 and MA_True==-1:MA_Cross=-1
    else:MA_Cross=0
    return [lastclose,mashort,malong,MA_True,MA_Cross]

if __name__ == '__main__':
    N=5
    M=10
    df=pd.read_csv('C:\\testdata\MacdMa_RB_xbardf_1.csv')
    df['ema']=calEMA(df['close'],50)
    print df.head(20)
    df.to_csv('eam_test.csv')
    #import numpy
    #for i in numpy.arange(21,344):
    #    df1.loc[i - 1]=newMA(df.iloc[0:i]['close'],df1,N,M)
    #df1.to_csv('EMA.csv')


#df=pd.read_csv('ta-macd-after.csv')
'''
#MACD测试
close=df['close']
df2=pd.DataFrame()
df2['close']=close

for i in range(1,30):
    data=close[0:i]
    m,d,b,s,l=calMACD(data)
    df2[str(i)]=m
df2.to_csv('macd_test.csv')
'''
'''
macd2,dea2,bar2,sema,lema=calMACD(df['close'])
df['MACD']=macd2
df['DEA']=dea2
df['BAR']=bar2
df['sema']=sema
df['lema']=lema
print macd2[-10:]
df.to_csv('ta-macd-after.csv')
'''
'''
ta-lib计算方法
import talib
ta_macd, ta_signal, ta_hist = talib.MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
df['ta-MACD']=ta_macd
df['ta-DIF']=ta_signal
df['ta-DEA']=ta_hist
print ta_macd[-10:]
'''
'''
#KDJ测试
kdjk,kdjd,kdjj,rsv,ll,hl=calKDJ(df)
df2=pd.DataFrame()
df2['time']=df['start_time']
df2['low']=df['low']
df2['high']=df['high']
df2['close']=df['close']
df2['RSV']=rsv
df2['lowL']=ll
df2['highL']=hl
df2['KDJ_K']=kdjk
df2['KDJ_D']=kdjd
df2['KDJ_J']=kdjj
df2.to_csv('kdj_test.csv')
'''
'''
#KDJ单个测试
#从读50个开始，使用单个计算的方法重新计算后面50个的值
df=pd.read_csv('ta-macd-after.csv')
df2=pd.read_csv('kdj_test.csv')
for i in range(52,100):
    calNewKDJ(df[0:i],df2)
df2.to_csv('newKDF_test_result.csv')
'''