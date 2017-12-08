# -*- coding: utf-8 -*-
import pandas as pd
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

def GET_DATA(datatype=DATA_TYPE_RAW,symbol='SHFE.RB',K_MIN=60,startdate='2017-05-01'):
    if datatype==DATA_TYPE_RAW:
        filename=BAR_DATA_PATH+symbol+'\\'+symbol+' '+str(K_MIN)+'.csv'
    elif datatype==DATA_TYPE_TICKS:
        filename=TICKS_DATA_PATH+symbol+'\\'+symbol+'ticks '+str(K_MIN)+'.csv'
    else:
        return
    df=pd.read_csv(filename)
    #df.drop('Unnamed: 0.1', axis=1, inplace=True)
    df.index=pd.to_datetime(df['utc_time'],unit='s')
    df = df.tz_localize(tz='PRC')
    df=df.truncate(before=startdate)
    print 'get data success '+symbol+str(K_MIN)+startdate
    return df

def getContractSwaplist(symbol):
    datapath=Collection_Path+'vitualContract\\'
    df=pd.read_csv(datapath+symbol+'ContractSwap.csv')
    return df
    pass