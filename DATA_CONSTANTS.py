# -*- coding: utf-8 -*-
import pandas as pd
import time
import os
#读取中文路径
Collection_Path=unicode('D:\\002 MakeLive\DataCollection\\','utf-8')
PUBLIC_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\public data\\','utf-8')
RAW_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\raw data\\','utf-8')
TICKS_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\ticks data\\','utf-8')
BAR_DATA_PATH=unicode('D:\\002 MakeLive\DataCollection\\bar data\\','utf-8')

TICKS_DATA_START_DATE='2017-8-17'#包含了8-17日
LAST_CONCAT_DATA='2017-10-17'#记录上次汇总数据的时间，不包含当天（要再加上一天，要不然后面truncate会不对）

DATA_TYPE_PUBLIC=1
DATA_TYPE_RAW=2
DATA_TYPE_TICKS=3

def getBarData(symbol='SHFE.RB',K_MIN=60,starttime='2017-05-01 00:00:00',endtime='2018-01-01 00:00:00'):

    filename=BAR_DATA_PATH+symbol+'\\'+symbol+' '+str(K_MIN)+'.csv'
    df=pd.read_csv(filename)
    startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(endtime,"%Y-%m-%d %H:%M:%S")))
    '''
    df.index=pd.to_datetime(df['utc_time'],unit='s')
    df = df.tz_localize(tz='PRC')
    df=df.truncate(before=startdate)
    '''
    df=df.loc[(df['utc_time']>startutc) & (df['utc_time']<endutc)]
    df['Unnamed: 0'] = range(0, df.shape[0])
    df.drop('Unnamed: 0.1.1', inplace=True,axis=1)
    df.reset_index(drop=True,inplace=True)
    #print 'get data success '+symbol+str(K_MIN)+startdate
    return df

def getTickData(symbol='SHFE.RB',K_MIN=60,startdate='2017-05-01',enddate='2018-01-01'):

    filename=TICKS_DATA_PATH+symbol+'\\'+symbol+'ticks '+str(K_MIN)+'.csv'
    df=pd.read_csv(filename)
    starttime=startdate+" 00:00:00"
    endtime= enddate+" 00:00:00"
    startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(endtime,"%Y-%m-%d %H:%M:%S")))
    df=df.loc[(df['utc_time']>startutc) & (df['utc_time']<endutc)]
    df['Unnamed: 0'] = range(0, df.shape[0])
    df.drop('Unnamed: 0.1.1',drop=True,inplace=True)
    df.reset_index(drop=True,inplace=True)
    #print 'get data success '+symbol+str(K_MIN)+startdate
    return df

def getContractSwaplist(symbol):
    datapath=Collection_Path+'vitualContract\\'
    df=pd.read_csv(datapath+symbol+'ContractSwap.csv')
    return df
    pass

def getCurrentPath():
    '''
    返回当前文件所在路径
    :return:
    '''
    return os.path.abspath('.')

def getUpperPath():
    '''
    返回当前文件所在的上一级路径
    :return:
    '''
    return os.path.abspath('..')

if __name__ == '__main__':
    df=getBarData("SHFE.RB",K_MIN=600,starttime='2011-10-08 00:00:00',endtime='2013-03-20 00:00:00')
    print df.head(10)
    print df.tail(10)