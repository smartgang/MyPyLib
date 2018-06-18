# -*- coding: utf-8 -*-
import pandas as pd
import time
import os

# 读取中文路径
Collection_Path = unicode('D:\\002 MakeLive\DataCollection\\', 'utf-8')
PUBLIC_DATA_PATH = unicode('D:\\002 MakeLive\DataCollection\public data\\', 'utf-8')
RAW_DATA_PATH = unicode('D:\\002 MakeLive\DataCollection\\raw data\\', 'utf-8')
TICKS_DATA_PATH = unicode('D:\\002 MakeLive\DataCollection\\ticks data\\', 'utf-8')
BAR_DATA_PATH = unicode('D:\\002 MakeLive\DataCollection\\bar data\\', 'utf-8')
VOLUME_DATA_PATH = unicode('D:\\002 MakeLive\DataCollection\\volume data\\', 'utf-8')

TICKS_DATA_START_DATE = '2017-8-17'  # 包含了8-17日
LAST_CONCAT_DATA = '2017-10-17'  # 记录上次汇总数据的时间，不包含当天（要再加上一天，要不然后面truncate会不对）

DATA_TYPE_PUBLIC = 1
DATA_TYPE_RAW = 2
DATA_TYPE_TICKS = 3


def getTradedates(exchangeid='SHFE', startdate='2016-01-01', enddate='2017-12-30'):
    # 获取交易所的交易日
    # 原文件保存在public data文件夹中
    startutc = float(time.mktime(time.strptime(startdate + ' 00:00:00', "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(enddate + ' 23:59:59', "%Y-%m-%d %H:%M:%S")))
    tradedatedf = pd.read_csv(PUBLIC_DATA_PATH + 'TradeDates.csv', index_col='exchange_id')
    df = tradedatedf.loc[(tradedatedf['utc_time'] >= startutc) & (tradedatedf['utc_time'] < endutc)]
    df = df.loc[exchangeid, :]
    df.reset_index(inplace=True)
    df.drop('Unnamed: 0', inplace=True, axis=1)
    return df


def generatDailyClose(dailyK):
    '''获取交易区间时间范围内的交易日和收盘价信息，生成dailyDf'''
    dailyK['date'] = dailyK['strtime'].str.slice(0, 10)
    closegrouped = dailyK['close'].groupby(dailyK['date'])
    utcgrouped = dailyK['utc_time'].groupby(dailyK['date'])
    dailyClose = pd.DataFrame(closegrouped.last())
    dailyClose['preclose'] = dailyClose['close'].shift(1).fillna(0)
    dailyClose['utc_time'] = utcgrouped.last()
    return dailyClose


# ---------------------------------------------------------------------------------------------
def getBarData(symbol='SHFE.RB', K_MIN=60, starttime='2017-05-01 00:00:00', endtime='2018-01-01 00:00:00'):
    # 读取bar数据
    filename = BAR_DATA_PATH + symbol + '\\' + symbol + ' ' + str(K_MIN) + '.csv'
    df = pd.read_csv(filename)
    startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
    '''
    df.index=pd.to_datetime(df['utc_time'],unit='s')
    df = df.tz_localize(tz='PRC')
    df=df.truncate(before=startdate)
    '''
    df = df.loc[(df['utc_time'] > startutc) & (df['utc_time'] < endutc)]
    df['Unnamed: 0'] = range(0, df.shape[0])
    # df.drop('Unnamed: 0.1', inplace=True,axis=1)
    df.reset_index(drop=True, inplace=True)
    # print 'get data success '+symbol+str(K_MIN)+startdate
    return df


def getBarBySymbol(domain_symbol, symbol, bar_type, starttime=None, endtime=None):
    # 取单个主力合约的数据
    filename = BAR_DATA_PATH + domain_symbol + '\\' + symbol + ' ' + str(bar_type) + '.csv'
    df = pd.read_csv(filename)
    if starttime:
        startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
        df = df.loc[df['utc_time'] >= startutc]
    if endtime:
        endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
        df = df.loc[df['utc_time'] <= endutc]
    df.reset_index(drop=True, inplace=True)
    return df


def getBarBySymbolList(domain_symbol, symbollist, bar_type, startdate=None, enddate=None):
    # 取全部主力合约的数据，以dic的形式返回
    bardic = {}
    startutc = None
    endutc = None
    if startdate:
        # 过滤掉主力结束时间在开始时间之前的，只取主力结束时间在开始时间之后
        startutc = float(time.mktime(time.strptime(startdate + " 00:00:00", "%Y-%m-%d %H:%M:%S")))
    if enddate:
        # 过滤掉主力开始时间在结束时间之后的，只取主力开始时间在结束时间之前
        endutc = float(time.mktime(time.strptime(enddate + " 23:59:59", "%Y-%m-%d %H:%M:%S")))
    for symbol in symbollist:
        filename = BAR_DATA_PATH + domain_symbol + '\\' + symbol + ' ' + str(bar_type) + '.csv'
        bardf = pd.read_csv(filename)
        if startutc:
            bardf = bardf.loc[bardf['utc_time'] >= startutc]
        if endutc:
            bardf = bardf.loc[bardf['utc_time'] <= endutc]
        bardic[symbol] = bardf
    return bardic

def getBarDic(symbolinfo, bar_type):
    # 取全部主力合约的数据，以dic的形式返回
    domain_symbol = symbolinfo.domain_symbol
    symbollist = symbolinfo.getSymbolList()
    bardic = {}
    startutc , endutc = symbolinfo.getUtcRange()
    for symbol in symbollist:
        domain_utc_start, domain_utc_end = symbolinfo.getSymbolDomainUtc(symbol)
        filename = BAR_DATA_PATH + domain_symbol + '\\' + symbol + ' ' + str(bar_type) + '.csv'
        bardf = pd.read_csv(filename)
        bardf = bardf.loc[bardf['utc_time']>=domain_utc_start]  # 只取主力时间之后的数据，以减少总的数据量
        if startutc:
            bardf = bardf.loc[bardf['utc_time'] >= startutc]
        if endutc:
            bardf = bardf.loc[bardf['utc_time'] <= endutc]
        bardic[symbol] = bardf
    return bardic

def getDomainbarByDomainSymbol(symbollist, bardic, symbolDomaindic):
    # 根据symbolDomaindic中每个合约的时间范围，从bardic中取数组合成主连数据
    # 默认双边的symbol是对得上的，不做检查
    domain_bar = pd.DataFrame()
    barlist = []
    #timestart = time.time()
    for symbol in symbollist:
        utcs = symbolDomaindic[symbol]
        bars = bardic[symbol]
        symbol_domain_start = utcs[0]
        symbol_domain_end = utcs[1]
        bar = bars.loc[(bars['utc_time'] >= symbol_domain_start) & (bars['utc_endtime'] < symbol_domain_end)]
        #domain_bar = pd.concat([domain_bar, bar])
        #domain_bar = domain_bar.append(bar)
        barlist.append(bar)
    #timebar = time.time()
    #print ("timebar %.3f" % (timebar - timestart))
    domain_bar = pd.concat(barlist)
    #timeconcat = time.time()
    #print ("timeconcat %.3f" % (timeconcat - timebar))
    #domain_bar.sort_values('utc_time',inplace=True)    # 本来有sort会妥当一点，不过sort比较耗时，就去掉了
    #timesort = time.time()
    #print ("timesort %.3f" % (timesort - timeconcat))
    domain_bar.reset_index(drop=True, inplace=True)
    #timeindex = time.time()
    #print ("timeindex %.3f" % (timeindex - timeconcat))
    return domain_bar


def getVolumeData(symbol='SHFE.RB', K_MIN=60, starttime='2017-05-01 00:00:00', endtime='2018-01-01 00:00:00'):
    # 读取bar数据
    filename = VOLUME_DATA_PATH + symbol + '\\' + symbol + ' ' + str(K_MIN) + '_volume.csv'
    df = pd.read_csv(filename)
    startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
    df = df.loc[(df['utc_time'] > startutc) & (df['utc_time'] < endutc)]
    df['Unnamed: 0'] = range(0, df.shape[0])
    # df.drop('Unnamed: 0.1', inplace=True,axis=1)
    df.reset_index(drop=True, inplace=True)
    # print 'get data success '+symbol+str(K_MIN)+startdate
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


def getTickByDate(symbol='SHFE.RB', tradedate='2017-08-07'):
    filename = TICKS_DATA_PATH + symbol + '\\' + symbol + tradedate + 'ticks.csv'
    df = pd.read_csv(filename)
    return df


def getContractSwaplist(symbol):
    datapath = Collection_Path + 'vitualContract\\'
    df = pd.read_csv(datapath + symbol + 'ContractSwap.csv')
    return df
    pass


# ----------------------------------------------------------
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
    p = '/'.join(['..'] * uppernume)
    return os.path.abspath(p)


# -------------------------------------------------------------
def getPriceTick(symbol):
    '''
    查询品种的最小价格变动
    :param symbol:
    :return:
    '''
    contract = pd.read_excel(PUBLIC_DATA_PATH + 'Contract.xlsx', index_col='Contract')
    return contract.ix[symbol, 'price_tick']


def getMultiplier(symbol):
    '''
    查询品种的合约乘数
    :param symbol:
    :return:
    '''
    contract = pd.read_excel(PUBLIC_DATA_PATH + 'Contract.xlsx', index_col='Contract')
    return contract.ix[symbol, 'multiplier']


def getMarginRatio(symbol):
    '''
    查询品种的保证金率
    :param symbol:
    :return:
    '''
    contract = pd.read_excel(PUBLIC_DATA_PATH + 'Contract.xlsx', index_col='Contract')
    return contract.ix[symbol, 'margin_ratio']


def getSlip(symbol):
    '''
    查询品种配置的滑点
    :param symbol:
    :return:
    '''
    contract = pd.read_excel(PUBLIC_DATA_PATH + 'Contract.xlsx', index_col='Contract')
    return contract.ix[symbol, 'slip']


class SymbolInfo:
    POUNDGE_TYPE_HAND = u'hand'
    POUNDGE_TYPE_RATE = u'rate'

    '''合约信息类'''

    def __init__(self, domain_symbol, startdate=None, enddate=None):
        self.domain_symbol = domain_symbol
        contract = pd.read_excel(PUBLIC_DATA_PATH + 'domainMap.xlsx', index_col='symbol')
        contractMapDf = pd.read_csv(PUBLIC_DATA_PATH + 'contractMap.csv', index_col='symbol')
        self.start_utc = None
        self.end_utc = None
        self.contractMap = contractMapDf.loc[contractMapDf['domain_symbol'] == domain_symbol]  # 取该主力合约编号对应的合约列表
        if startdate:
            # 过滤掉主力结束时间在开始时间之前的，只取主力结束时间在开始时间之后
            self.start_utc = float(time.mktime(time.strptime(startdate+ " 00:00:00", "%Y-%m-%d %H:%M:%S")))
            self.contractMap = self.contractMap.loc[self.contractMap['domain_end_utc'] > self.start_utc]
        if enddate:
            # 过滤掉主力开始时间在结束时间之后的，只取主力开始时间在结束时间之前
            self.end_utc = float(time.mktime(time.strptime(enddate + " 23:59:59", "%Y-%m-%d %H:%M:%S")))
            self.contractMap = self.contractMap.loc[self.contractMap['domain_start_utc'] < self.end_utc]

        self.contractMap = self.contractMap.sort_values('domain_start_utc')  # 根据主力时间排序

        self.active = contract.ix[domain_symbol, 'active']  # 激活标志
        self.priceTick = contract.ix[domain_symbol, 'price_tick']
        self.multiplier = contract.ix[domain_symbol, 'multiplier']
        self.marginRatio = contract.ix[domain_symbol, 'margin_ratio']
        self.slip = contract.ix[domain_symbol, 'slip']
        self.poundageType = contract.ix[domain_symbol, 'poundage_type']
        self.poundageFee = contract.ix[domain_symbol, 'poundage_fee']
        self.poundageRate = contract.ix[domain_symbol, 'poundage_rate']

    def getPriceTick(self):
        return self.priceTick

    def getMultiplier(self):
        return self.multiplier

    def getMarginRatio(self):
        return self.marginRatio

    def getSlip(self):
        return self.slip

    def getPoundage(self):
        return self.poundageType, self.poundageFee, self.poundageRate

    def getSymbolList(self):
        return self.contractMap.index.tolist()

    def getSymbolDomainUtc(self, symbol):
        return self.contractMap.ix[symbol, 'domain_start_utc'], self.contractMap.ix[symbol, 'domain_end_utc']

    def getSymbolDomainTime(self, symbol):
        return self.contractMap.ix[symbol, 'domain_start_date'], self.contractMap.ix[symbol, 'domain_end_date']

    def getUtcRange(self):
        return self.start_utc, self.end_utc

    def getSymbolDomainDic(self):
        domainDic = {}
        symbolList = self.getSymbolList()
        for symbol in symbolList:
            s, e = self.getSymbolDomainUtc(symbol)
            domainDic[symbol] = [s, e]
        return domainDic

    def amendSymbolDomainDicByOpr(self, oprdf):
        # 基于传入的oprdf修正symbolDomainDic,因为合约切换时，会有持仓未平仓导致上一合约实际生效时间超过其主力结束时间的现象，故要修改正symbolDomainDic
        # 注：可能会有些合适期间没有opr的情况，所以symbolList会比opr中的symbollist少
        oprgrouped = oprdf.groupby('symbol')
        symbol_last_utc_list = oprgrouped['closeutc'].last()
        opr_symbol_list = symbol_last_utc_list.index.tolist()
        symbol_last_utc = None
        domainDic = {}
        symbolList = self.getSymbolList()
        for symbol in symbolList:
            s, e = self.getSymbolDomainUtc(symbol)
            if symbol_last_utc:
                s = symbol_last_utc
            if symbol in opr_symbol_list:
                symbol_last_utc = symbol_last_utc_list[symbol]
            if symbol_last_utc and symbol_last_utc > e:
                e = symbol_last_utc
            else:
                symbol_last_utc = None
            domainDic[symbol] = [s, e]
        return domainDic

    def isActive(self):
        return self.active


class TickDataSupplier:

    def __init__(self, symbol, startdate, enddate):
        self.startdate = startdate
        self.enddate = enddate
        self.startdateutc = float(time.mktime(time.strptime(startdate + ' 00:00:00', "%Y-%m-%d %H:%M:%S")))
        self.enddateutc = float(time.mktime(time.strptime(enddate + ' 23:59:59', "%Y-%m-%d %H:%M:%S")))
        self.symbol = symbol
        self.exchange, self.secid = symbol.split('.', 1)
        self.datelist = getTradedates(self.exchange, self.startdate, self.enddate)['strtime']
        self.tickdatadf = pd.DataFrame()
        for d in self.datelist:
            print 'Collecting tick data:', d
            self.tickdatadf = pd.concat([self.tickdatadf, getTickByDate(self.symbol, d)])

    def getTickData(self, starttime, endtime):
        startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
        endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
        '''
        df.index=pd.to_datetime(df['utc_time'],unit='s')
        df = df.tz_localize(tz='PRC')
        df=df.truncate(before=startdate)
        '''
        df = self.tickdatadf.loc[(self.tickdatadf['utc_time'] > startutc) & (self.tickdatadf['utc_time'] < endutc)]
        df['Unnamed: 0'] = range(0, df.shape[0])
        # df.drop('Unnamed: 0.1.1', inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)
        return df

    def getTickDataByUtc(self, startutc, endutc):
        df = self.tickdatadf.loc[(self.tickdatadf['utc_time'] > startutc) & (self.tickdatadf['utc_time'] < endutc)]
        df['Unnamed: 0'] = range(0, df.shape[0])
        # df.drop('Unnamed: 0.1.1', inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)
        return df

    def getDateRange(self):
        return self.startdate, self.enddate

    def getDateUtcRange(self):
        return self.startdateutc, self.enddateutc

    def getSymbol(self):
        return self.symbol

    def getDateList(self):
        return self.datelist


def symbolInfoTest():
    domain_symbol = 'SHFE.RB'
    symbolinfo = SymbolInfo(domain_symbol)
    symbollist = symbolinfo.getSymbolList()
    print symbolinfo.getSymbolDomainDic()
    print symbolinfo.isActive()
    bardic = getBarBySymbolList(domain_symbol, symbollist, 3600)
    for symbol in symbollist:
        print bardic[symbol].head(5)


# ========================================================================================
if __name__ == '__main__':
    # df=getBarData("SHFE.RB",K_MIN=600,starttime='2011-10-08 00:00:00',endtime='2013-03-20 00:00:00')
    # df=getTradedates('SHFE','2017-10-01','2017-12-12')
    # ticksupplier = TickDataSupplier('SHFE.RB', '2017-10-01', '2017-12-10')
    # df1 = ticksupplier.getTickData('2017-10-01 00:00:00', '2017-12-03 22:10:15')
    # print df1.head(10)
    # print df1.tail(10)
    symbolInfoTest()
