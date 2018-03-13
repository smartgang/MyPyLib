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



def getTradedates(exchangeid='SHFE',startdate='2016-01-01',enddate='2017-12-30'):
    #获取交易所的交易日
    #原文件保存在public data文件夹中
    startutc = float(time.mktime(time.strptime(startdate+' 00:00:00', "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(enddate+' 23:30:00',"%Y-%m-%d %H:%M:%S")))
    tradedatedf=pd.read_csv(PUBLIC_DATA_PATH+'TradeDates.csv',index_col='exchange_id')
    df = tradedatedf.loc[(tradedatedf['utc_time'] > startutc) & (tradedatedf['utc_time'] < endutc)]
    df=df.loc[exchangeid,:]
    df.reset_index(inplace=True)
    df.drop('Unnamed: 0', inplace=True, axis=1)
    return df

#---------------------------------------------------------------------------------------------
def getBarData(symbol='SHFE.RB',K_MIN=60,starttime='2017-05-01 00:00:00',endtime='2018-01-01 00:00:00'):
    #读取bar数据
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
    #df.drop('Unnamed: 0.1', inplace=True,axis=1)
    df.reset_index(drop=True,inplace=True)
    #print 'get data success '+symbol+str(K_MIN)+startdate
    return df

'''
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
'''
def getTickByDate(symbol='SHFE.RB',tradedate='2017-08-07'):
    filename=TICKS_DATA_PATH+symbol+'\\'+symbol+tradedate+'ticks.csv'
    df=pd.read_csv(filename)
    return df


def getContractSwaplist(symbol):
    datapath=Collection_Path+'vitualContract\\'
    df=pd.read_csv(datapath+symbol+'ContractSwap.csv')
    return df
    pass
#----------------------------------------------------------
def getCurrentPath():
    '''
    返回当前文件所在路径
    :return:
    '''
    return os.path.abspath('.')

def getUpperPath(uppernume=1):
    '''
    返回当前文件所在的上一级路径
    :return:
    '''
    p='/'.join(['..']*uppernume)
    return os.path.abspath(p)

#-------------------------------------------------------------
def getPriceTick(symbol):
    '''
    查询品种的最小价格变动
    :param symbol:
    :return:
    '''
    contract=pd.read_excel(PUBLIC_DATA_PATH+'Contract.xlsx',index_col='Contract')
    return contract.ix[symbol,'price_tick']

def getMultiplier(symbol):
    '''
    查询品种的合约乘数
    :param symbol:
    :return:
    '''
    contract=pd.read_excel(PUBLIC_DATA_PATH+'Contract.xlsx',index_col='Contract')
    return contract.ix[symbol,'multiplier']

def getMarginRatio(symbol):
    '''
    查询品种的保证金率
    :param symbol:
    :return:
    '''
    contract=pd.read_excel(PUBLIC_DATA_PATH+'Contract.xlsx',index_col='Contract')
    return contract.ix[symbol,'margin_ratio']

def getSlip(symbol):
    '''
    查询品种配置的滑点
    :param symbol:
    :return:
    '''
    contract=pd.read_excel(PUBLIC_DATA_PATH+'Contract.xlsx',index_col='Contract')
    return contract.ix[symbol,'slip']

class SymbolInfo:

    POUNDGE_TYPE_HAND = u'hand'
    POUNDGE_TYPE_RATE = u'rate'

    '''合约信息类'''
    def __init__(self,symbol):
        self.symbol=symbol
        contract = pd.read_excel(PUBLIC_DATA_PATH + 'Contract.xlsx', index_col='Contract')
        self.priceTick=contract.ix[symbol, 'price_tick']
        self.multiplier=contract.ix[symbol, 'multiplier']
        self.marginRatio=contract.ix[symbol, 'margin_ratio']
        self.slip=contract.ix[symbol, 'slip']
        self.poundageType=contract.ix[symbol,'poundage_type']
        self.poundageFee = contract.ix[symbol,'poundage_fee']
        self.poundageRate = contract.ix[symbol,'poundage_rate']

    def getPriceTick(self):
        return self.priceTick

    def getMultiplier(self):
        return self.multiplier

    def getMarginRatio(self):
        return self.marginRatio

    def getSlip(self):
        return self.slip

    def getPoundage(self):
        return self.poundageType,self.poundageFee,self.poundageRate

class TickDataSupplier:

    def __init__(self,symbol,startdate,enddate):
        self.startdate=startdate
        self.enddate=enddate
        self.startdateutc = float(time.mktime(time.strptime(startdate+' 00:00:00', "%Y-%m-%d %H:%M:%S")))
        self.enddateutc = float(time.mktime(time.strptime(enddate+' 23:59:59', "%Y-%m-%d %H:%M:%S")))
        self.symbol=symbol
        self.exchange,self.secid=symbol.split('.',1)
        self.datelist=getTradedates(self.exchange,self.startdate,self.enddate)['strtime']
        self.tickdatadf=pd.DataFrame()
        for d in self.datelist:
            print 'Collecting tick data:',d
            self.tickdatadf=pd.concat([self.tickdatadf,getTickByDate(self.symbol,d)])

    def getTickData(self,starttime,endtime):
        startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
        endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
        '''
        df.index=pd.to_datetime(df['utc_time'],unit='s')
        df = df.tz_localize(tz='PRC')
        df=df.truncate(before=startdate)
        '''
        df = self.tickdatadf.loc[(self.tickdatadf['utc_time'] > startutc) & (self.tickdatadf['utc_time'] < endutc)]
        df['Unnamed: 0'] = range(0, df.shape[0])
        #df.drop('Unnamed: 0.1.1', inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)
        return df

    def getTickDataByUtc(self,startutc,endutc):

        df = self.tickdatadf.loc[(self.tickdatadf['utc_time'] > startutc) & (self.tickdatadf['utc_time'] < endutc)]
        df['Unnamed: 0'] = range(0, df.shape[0])
        #df.drop('Unnamed: 0.1.1', inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)
        return df

    def getDateRange(self):
        return self.startdate,self.enddate

    def getDateUtcRange(self):
        return self.startdateutc,self.enddateutc

    def getSymbol(self):
        return self.symbol

    def getDateList(self):
        return self.datelist

#========================================================================================
if __name__ == '__main__':
    #df=getBarData("SHFE.RB",K_MIN=600,starttime='2011-10-08 00:00:00',endtime='2013-03-20 00:00:00')
    #df=getTradedates('SHFE','2017-10-01','2017-12-12')
    ticksupplier=TickDataSupplier('SHFE.RB','2017-10-01','2017-12-10')
    df1=ticksupplier.getTickData('2017-10-01 00:00:00','2017-12-03 22:10:15')
    print df1.head(10)
    print df1.tail(10)