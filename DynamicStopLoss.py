# -*- coding: utf-8 -*-
'''
只能做到min级
遍历oprlist，取出startutc和endutc，根据utc取1min的数据
如果是多仓，算1min数据close的最大回撤，作为最大期间亏损
如果是空仓，算1min数据close的最大反回撤，作为最大期间亏损
如果最大期间亏损大于阀值，则根据最大值和阀值round出一个平仓价格
附带统计：
统计每个操作期间的最大收益和最大亏损
'''
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import os
import ResultStatistics as RS
import multiprocessing

def max_draw(bardf):
    '''
    根据close的最大回撤值和比例
    :param df:
    :return:
    '''
    df=pd.DataFrame({'close':bardf.close,'strtime':bardf['strtime'],'utc_time':bardf['utc_time'],'timeindex':bardf['Unnamed: 0']})

    df['max2here']=df['close'].expanding().max()
    df['dd2here']=df['close']/df['max2here']-1

    temp= df.sort_values(by='dd2here').iloc[0]
    max_dd=temp['dd2here']
    max_dd_close=temp['close']
    max = temp['max2here']
    strtime = temp['strtime']
    utctime = temp['utc_time']
    timeindex = temp['timeindex']
    #返回值为最大回撤比例，最大回撤价格，最大回撤的最高价,最大回撤时间和位置
    return max_dd,max_dd_close,max,strtime,utctime,timeindex

def max_reverse_draw(bardf):
    df = pd.DataFrame({'close': bardf.close, 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})

    df['min2here']=df['close'].expanding().min()
    df['dd2here']=1-df['close']/df['min2here']

    temp= df.sort_values(by='dd2here').iloc[0]
    max_dd=temp['dd2here']
    max_dd_close=temp['close']
    min = temp['min2here']
    strtime = temp['strtime']
    utctime = temp['utc_time']
    timeindex = temp['timeindex']
    return max_dd,max_dd_close,min,strtime,utctime,timeindex

def getLongDrawback(bardf,stopTarget):
    df = pd.DataFrame({'close': bardf.close, 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['max2here']=df['close'].expanding().max()
    df['dd2here'] = df['close'] / df['max2here'] - 1
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['close']
        maxprice = temp['max2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex

def getShortDrawback(bardf,stopTarget):
    df = pd.DataFrame({'close': bardf.close, 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['min2here']=df['close'].expanding().min()
    df['dd2here'] = 1 - df['close'] / df['min2here']
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['close']
        maxprice = temp['min2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex

def getLongDrawbackByRealtick(tickdf,stopTarget):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time'],
                       'timeindex': tickdf['Unnamed: 0']})
    df['max2here']=df['close'].expanding().max()
    df['dd2here'] = df['close'] / df['max2here'] - 1
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['close']
        maxprice = temp['max2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex

def getShortDrawbackByRealtick(tickdf,stopTarget):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time'],
                       'timeindex': tickdf['Unnamed: 0']})
    df['min2here']=df['close'].expanding().min()
    df['dd2here'] = 1 - df['close'] / df['min2here']
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['close']
        maxprice = temp['min2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex



def getLongDrawbackByTick(bardf,stopTarget):
    df = pd.DataFrame({'high': bardf['longHigh'],'low':bardf['longLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['max2here']=df['high'].expanding().max()
    df['dd2here'] = df['low'] / df['max2here'] - 1
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['low']
        maxprice = temp['max2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex

def getShortDrawbackByTick(bardf,stopTarget):
    df = pd.DataFrame({'high': bardf['shortHigh'],'low':bardf['shortLow'] ,'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['min2here']=df['low'].expanding().min()
    df['dd2here'] = 1 - df['high'] / df['min2here']
    df['dd'] = df['dd2here'] - stopTarget
    tempdf = df.loc[df['dd']<0]
    if tempdf.shape[0]>0:
        temp = tempdf.iloc[0]
        max_dd = temp['dd2here']
        max_dd_close = temp['high']
        maxprice = temp['min2here']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
    else:
        max_dd = 0
        max_dd_close = 0
        maxprice = 0
        strtime = ' '
        utctime = 0
        timeindex = 0
    return max_dd,max_dd_close,maxprice,strtime,utctime,timeindex


def dslCal(symbol,K_MIN,setname,bar1m,barxm,pricetick,slip,slTarget,tofolder):
    print 'sl;', str(slTarget), ',setname:', setname
    oprdf = pd.read_csv(symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprdf['max_opr_gain'] = 0 #本次操作期间的最大收益
    oprdf['min_opr_gain'] = 0#本次操作期间的最小收益
    oprdf['max_dd'] = 0
    oprnum = oprdf.shape[0]
    worknum=0
    for i in range(oprnum):
        opr = oprdf.iloc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60#从开仓的10m线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime#一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        data1m = bar1m.loc[(bar1m['utc_time'] >= startutc) & (bar1m['utc_time'] < endutc)]
        if oprtype == 1:
            # 多仓，取最大回撤，max为最大收益，min为最小收益
            max_dd, dd_close, maxprice, strtime, utctime, timeindex = getLongDrawbackByTick(data1m, slTarget)
            oprdf.ix[i, 'max_opr_gain'] = (data1m.high.max() - openprice) / openprice#1min用close,tick用high和low
            oprdf.ix[i, 'min_opr_gain'] = (data1m.low.min() - openprice) / openprice
            oprdf.ix[i, 'max_dd'] = max_dd
            if max_dd <= slTarget:
                ticknum = round((maxprice * slTarget) / pricetick, 0) - 1
                oprdf.ix[i, 'new_closeprice'] = maxprice + ticknum * pricetick
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

        else:
            # 空仓，取逆向最大回撤，min为最大收益，max为最小收闪
            max_dd, dd_close, minprice, strtime, utctime, timeindex = getShortDrawbackByTick(data1m, slTarget)
            oprdf.ix[i, 'max_opr_gain'] = (openprice - data1m.low.min()) / openprice
            oprdf.ix[i, 'min_opr_gain'] = (openprice - data1m.high.max()) / openprice
            oprdf.ix[i, 'max_dd'] = max_dd
            if max_dd <= slTarget:
                ticknum = round((minprice * slTarget) / pricetick, 0) - 1
                oprdf.ix[i, 'new_closeprice'] = minprice - ticknum * pricetick
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

    initial_cash = 20000
    margin_rate = 0.2
    commission_ratio = 0.00012
    firsttradecash = initial_cash / margin_rate
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'] = firsttradecash * commission_ratio * 2
    oprdf['new_per earn'] = 0  # 单笔盈亏
    oprdf['new_own cash'] = 0  # 自有资金线
    oprdf['new_trade money'] = 0  # 杠杆后的可交易资金线
    oprdf['new_retrace rate'] = 0  # 回撤率

    oprdf.ix[0, 'new_per earn'] = firsttradecash * oprdf.ix[0, 'new_ret_r']
    maxcash = initial_cash + oprdf.ix[0, 'new_per earn'] - oprdf.ix[0, 'new_commission_fee']
    oprdf.ix[0, 'new_own cash'] = maxcash
    oprdf.ix[0, 'new_trade money'] = oprdf.ix[0, 'new_own cash'] / margin_rate
    oprtimes = oprdf.shape[0]
    for i in np.arange(1, oprtimes):
        commission = oprdf.ix[i - 1, 'new_trade money'] * commission_ratio * 2
        perearn = oprdf.ix[i - 1, 'new_trade money'] * oprdf.ix[i, 'new_ret_r']
        owncash = oprdf.ix[i - 1, 'new_own cash'] + perearn - commission
        maxcash = max(maxcash, owncash)
        retrace_rate = (maxcash - owncash) / maxcash
        oprdf.ix[i, 'new_own cash'] = owncash
        oprdf.ix[i, 'new_commission_fee'] = commission
        oprdf.ix[i, 'new_per earn'] = perearn
        oprdf.ix[i, 'new_trade money'] = owncash / margin_rate
        oprdf.ix[i, 'new_retrace rate'] = retrace_rate
    #保存新的result文档
    oprdf.to_csv(tofolder+symbol + str(K_MIN) + ' ' + setname + ' resultDSL_by_tick.csv')

    #计算统计结果
    oldendcash = oprdf.ix[oprnum - 1, 'own cash']
    oldAnnual = RS.annual_return(oprdf)
    oldSharpe = RS.sharpe_ratio(oprdf)
    oldDrawBack = RS.max_drawback(oprdf)[0]
    oldSR = RS.success_rate(oprdf)
    newendcash = oprdf.ix[oprnum - 1, 'new_own cash']
    newAnnual = RS.annual_return(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc')
    newSharpe = RS.sharpe_ratio(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc',retr_col='new_ret_r')
    newDrawBack = RS.max_drawback(oprdf,cash_col='new_own cash')[0]
    newSR = RS.success_rate(oprdf,ret_col='new_ret')
    max_single_loss_rate = abs(oprdf['new_ret_r'].min())
    max_retrace_rate = oprdf['new_retrace rate'].max()
    del oprdf
    return [setname,slTarget,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate,max_retrace_rate]

#======================================================================================
def fastDslCal(symbol,K_MIN,setname,bar1m,barxm,pricetick,slip,slTarget,tofolder):
    #快速动态止损
    print 'sl;', str(slTarget), ',setname:', setname
    oprdf = pd.read_csv(symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprdf['max_opr_gain'] = 0 #本次操作期间的最大收益
    oprdf['min_opr_gain'] = 0#本次操作期间的最小收益
    oprdf['max_dd'] = 0
    oprnum = oprdf.shape[0]
    worknum=0

    #1.数据对齐，在1m数据上标出买卖数据区间
    #先到10m数据中，找到utc_time对应的utc_endtime，作用对齐的标准
    barxm.set_index('utc_time',inplace=True)
    oprdf.set_index('openutc',drop=False,inplace=True)
    oprdf['openendutc']=barxm.loc[oprdf.index,'utc_endtime']-60

    oprdf.set_index('closeutc',drop=False,inplace=True)
    oprdf['closeendutc']=barxm.loc[oprdf.index,'utc_endtime']-60
    oprdf.reset_index(drop=True,inplace=True)
    #将Unanamed: 0列作为ID
    bar1m.set_index('utc_time',inplace=True)
    longopr=oprdf.loc[oprdf['tradetype']==1]
    shortopr = oprdf.loc[oprdf['tradetype']==-1]
    #对齐做多操作
    longopr.set_index('openendutc',inplace=True)
    bar1m['openlongID'] = longopr['Unnamed: 0']
    bar1m.fillna(method='ffill',inplace=True)
    bar1m.fillna('None', inplace=True)
    longopr.set_index('closeendutc',inplace=True)
    bar1m['closelongID'] = longopr['Unnamed: 0']
    bar1m.fillna(method='bfill', inplace=True)
    bar1m.fillna('None', inplace=True)
    bar1m['longID']=bar1m['openlongID']
    bar1m.loc[bar1m['openlongID']!=bar1m['closelongID'],'longID']='None'
    #对齐做空操作
    shortopr.set_index('openendutc',inplace=True)
    bar1m['openshortID'] = shortopr['Unnamed: 0']
    bar1m.fillna(method='ffill', inplace=True)
    bar1m.fillna('None', inplace=True)
    shortopr.set_index('closeendutc', inplace=True)
    bar1m['closeshortID'] = shortopr['Unnamed: 0']
    bar1m.fillna(method='bfill', inplace=True)
    bar1m.fillna('None', inplace=True)
    bar1m['shortID'] = bar1m['openshortID']
    bar1m.loc[bar1m['openshortID'] != bar1m['closeshortID'], 'shortID'] = 'None'
    bar1m.reset_index(inplace=True)

    bar1m.to_csv('bar1m.csv')

    #2.生成dsl结果的df
    longgroup=bar1m.groupby('longID').apply(getLongDrawbackByTick,(slTarget,))
    longlist=longgroup.tolist()
    longdf=pd.DataFrame(longlist,columns=['max_dd','max_dd_close','maxprice','strtime','utctime','timeindex'])
    longdf.index=longgroup.index
    longdf.drop('None',inplace=True)
    longdf = longdf.loc[longdf['maxprice'] != 0]  # 去掉dsl不生效的列
    longdf['slT']=longdf['maxprice']*slTarget
    longdf['ticknum']=longdf['slT'].floordiv(pricetick)
    longdf['new_closeprice']=longdf['maxprice'] + longdf['ticknum']*pricetick

    shortgroup=bar1m.groupby('shortID').apply(getShortDrawbackByTick,(slTarget,))
    shortlist=shortgroup.tolist()
    shortdf=pd.DataFrame(shortlist,columns=['max_dd','max_dd_close','maxprice','strtime','utctime','timeindex'])
    shortdf.index=shortgroup.index
    shortdf.drop('None', inplace=True)
    shortdf = shortdf.loc[shortdf['maxprice'] != 0]  # 去掉dsl不生效的列
    shortdf['slT']=shortdf['maxprice']*slTarget
    shortdf['ticknum']=shortdf['slT'].floordiv(pricetick)
    shortdf['new_closeprice']=shortdf['maxprice'] - shortdf['ticknum']*pricetick

    dsldf=pd.concat([longdf,shortdf])
    dsldf.sort_index(inplace=True)

    for i in dsldf.index.tolist():
        oprdf.ix[i,'new_closeprice'] = dsldf.ix[i,'new_closeprice']
        oprdf.ix[i,'new_closetime'] = dsldf.ix[i,'strtime']
        oprdf.ix[i,'new_closeindex'] = dsldf.ix[i,'timeindex']
        oprdf.ix[i,'new_closeutc'] = dsldf.ix[i,'utctime']

    initial_cash = 20000
    margin_rate = 0.2
    commission_ratio = 0.00012
    firsttradecash = initial_cash / margin_rate
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'] = firsttradecash * commission_ratio * 2
    oprdf['new_per earn'] = 0  # 单笔盈亏
    oprdf['new_own cash'] = 0  # 自有资金线
    oprdf['new_trade money'] = 0  # 杠杆后的可交易资金线
    oprdf['new_retrace rate'] = 0  # 回撤率

    oprdf.ix[0, 'new_per earn'] = firsttradecash * oprdf.ix[0, 'new_ret_r']
    maxcash = initial_cash + oprdf.ix[0, 'new_per earn'] - oprdf.ix[0, 'new_commission_fee']
    oprdf.ix[0, 'new_own cash'] = maxcash
    oprdf.ix[0, 'new_trade money'] = oprdf.ix[0, 'new_own cash'] / margin_rate
    oprtimes = oprdf.shape[0]
    for i in np.arange(1, oprtimes):
        commission = oprdf.ix[i - 1, 'new_trade money'] * commission_ratio * 2
        perearn = oprdf.ix[i - 1, 'new_trade money'] * oprdf.ix[i, 'new_ret_r']
        owncash = oprdf.ix[i - 1, 'new_own cash'] + perearn - commission
        maxcash = max(maxcash, owncash)
        retrace_rate = (maxcash - owncash) / maxcash
        oprdf.ix[i, 'new_own cash'] = owncash
        oprdf.ix[i, 'new_commission_fee'] = commission
        oprdf.ix[i, 'new_per earn'] = perearn
        oprdf.ix[i, 'new_trade money'] = owncash / margin_rate
        oprdf.ix[i, 'new_retrace rate'] = retrace_rate
    #保存新的result文档
    oprdf.to_csv(tofolder+symbol + str(K_MIN) + ' ' + setname + ' resultfastDSL_by_tick.csv')

    #计算统计结果
    oldendcash = oprdf.ix[oprnum - 1, 'own cash']
    oldAnnual = RS.annual_return(oprdf)
    oldSharpe = RS.sharpe_ratio(oprdf)
    oldDrawBack = RS.max_drawback(oprdf)[0]
    oldSR = RS.success_rate(oprdf)
    newendcash = oprdf.ix[oprnum - 1, 'new_own cash']
    newAnnual = RS.annual_return(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc')
    newSharpe = RS.sharpe_ratio(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc',retr_col='new_ret_r')
    newDrawBack = RS.max_drawback(oprdf,cash_col='new_own cash')[0]
    newSR = RS.success_rate(oprdf,ret_col='new_ret')
    max_single_loss_rate = abs(oprdf['new_ret_r'].min())
    max_retrace_rate = oprdf['new_retrace rate'].max()
    del oprdf
    return [setname,slTarget,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate,max_retrace_rate]


def dslCalRealTick(symbol,K_MIN,setname,ticksupplier,barxm,pricetick,slip,slTarget,tofolder):
    print 'sl;', str(slTarget), ',setname:', setname
    oprdf = pd.read_csv(symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    tickstartutc,tickendutc=ticksupplier.getDateUtcRange()
    #只截取tick时间范围内的opr
    oprdf = oprdf.loc[(oprdf['openutc'] > tickstartutc) & (oprdf['openutc'] < tickendutc)]

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprdf['max_opr_gain'] = 0 #本次操作期间的最大收益
    oprdf['min_opr_gain'] = 0#本次操作期间的最小收益
    oprdf['max_dd'] = 0
    #oprnum = oprdf.shape[0]
    oprindex=oprdf.index.tolist()
    worknum=0
    #for i in range(oprnum):
    for i in oprindex:
        opr = oprdf.loc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime#从开仓的10m线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime#一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        tickdata = ticksupplier.getTickDataByUtc(startutc,endutc)
        if oprtype == 1:
            # 多仓，取最大回撤，max为最大收益，min为最小收益
            max_dd, dd_close, maxprice, strtime, utctime, timeindex = getLongDrawbackByRealtick(tickdata, slTarget)
            oprdf.ix[i, 'max_opr_gain'] = (tickdata.last_price.max() - openprice) / openprice#1min用close,tick用high和low
            oprdf.ix[i, 'min_opr_gain'] = (tickdata.last_price.min() - openprice) / openprice
            oprdf.ix[i, 'max_dd'] = max_dd
            if max_dd <= slTarget:
                #ticknum = round((maxprice * slTarget) / pricetick, 0) - 1
                #oprdf.ix[i, 'new_closeprice'] = maxprice + ticknum * pricetick
                oprdf.ix[i,'new_closeprice'] = dd_close
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

        else:
            # 空仓，取逆向最大回撤，min为最大收益，max为最小收闪
            max_dd, dd_close, minprice, strtime, utctime, timeindex = getShortDrawbackByRealtick(tickdata, slTarget)
            oprdf.ix[i, 'max_opr_gain'] = (openprice - tickdata.last_price.min()) / openprice
            oprdf.ix[i, 'min_opr_gain'] = (openprice - tickdata.last_price.max()) / openprice
            oprdf.ix[i, 'max_dd'] = max_dd
            if max_dd <= slTarget:
                #ticknum = round((minprice * slTarget) / pricetick, 0) - 1
                #oprdf.ix[i, 'new_closeprice'] = minprice - ticknum * pricetick
                oprdf.ix[i,'new_closeprice'] = dd_close
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

    #initial_cash = 20000
    #margin_rate = 0.2
    #commission_ratio = 0.00012
    #firsttradecash = initial_cash / margin_rate
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['retdelta'] = oprdf['new_ret']-oprdf['ret']
    oprdf.to_csv(tofolder + symbol + str(K_MIN) + ' ' + setname + ' resultDSL_by_realtick.csv')
    '''
    oprdf['new_commission_fee'] = firsttradecash * commission_ratio * 2
    oprdf['new_per earn'] = 0  # 单笔盈亏
    oprdf['new_own cash'] = 0  # 自有资金线
    oprdf['new_trade money'] = 0  # 杠杆后的可交易资金线
    oprdf['new_retrace rate'] = 0  # 回撤率

    oprdf.ix[0, 'new_per earn'] = firsttradecash * oprdf.ix[0, 'new_ret_r']
    maxcash = initial_cash + oprdf.ix[0, 'new_per earn'] - oprdf.ix[0, 'new_commission_fee']
    oprdf.ix[0, 'new_own cash'] = maxcash
    oprdf.ix[0, 'new_trade money'] = oprdf.ix[0, 'new_own cash'] / margin_rate
    oprtimes = oprdf.shape[0]
    for i in np.arange(1, oprtimes):
        commission = oprdf.ix[i - 1, 'new_trade money'] * commission_ratio * 2
        perearn = oprdf.ix[i - 1, 'new_trade money'] * oprdf.ix[i, 'new_ret_r']
        owncash = oprdf.ix[i - 1, 'new_own cash'] + perearn - commission
        maxcash = max(maxcash, owncash)
        retrace_rate = (maxcash - owncash) / maxcash
        oprdf.ix[i, 'new_own cash'] = owncash
        oprdf.ix[i, 'new_commission_fee'] = commission
        oprdf.ix[i, 'new_per earn'] = perearn
        oprdf.ix[i, 'new_trade money'] = owncash / margin_rate
        oprdf.ix[i, 'new_retrace rate'] = retrace_rate
    #保存新的result文档
    oprdf.to_csv(tofolder+symbol + str(K_MIN) + ' ' + setname + ' resultDSL_by_tick.csv')

    #计算统计结果
    oldendcash = oprdf.ix[oprnum - 1, 'own cash']
    oldAnnual = RS.annual_return(oprdf)
    oldSharpe = RS.sharpe_ratio(oprdf)
    oldDrawBack = RS.max_drawback(oprdf)[0]
    oldSR = RS.success_rate(oprdf)
    newendcash = oprdf.ix[oprnum - 1, 'new_own cash']
    newAnnual = RS.annual_return(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc')
    newSharpe = RS.sharpe_ratio(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc',retr_col='new_ret_r')
    newDrawBack = RS.max_drawback(oprdf,cash_col='new_own cash')[0]
    newSR = RS.success_rate(oprdf,ret_col='new_ret')
    max_single_loss_rate = abs(oprdf['new_ret_r'].min())
    max_retrace_rate = oprdf['new_retrace rate'].max()

    return [setname,slTarget,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate,max_retrace_rate]
    '''
if __name__ == '__main__':
    import datetime
    #参数配置
    exchange_id = 'SHFE'
    sec_id='RB'
    symbol = '.'.join([exchange_id, sec_id])
    K_MIN = 600
    topN=5000
    pricetick=DC.getPriceTick(symbol)
    slip=pricetick
    starttime='2016-01-01'
    endtime='2018-03-31'
    #优化参数
    stoplossStep=-0.002
    #stoplossList = np.arange(-0.022, -0.042, stoplossStep)
    stoplossList=[-0.022]
    #文件路径

    currentpath=DC.getCurrentPath()
    bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=starttime+' 00:00:00',endtime=endtime+' 00:00:00')
    barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=starttime+' 00:00:00',endtime=endtime+' 00:00:00')
    #bar1m计算longHigh,longLow,shortHigh,shortLow
    bar1m['longHigh']=bar1m['high']
    bar1m['shortHigh']=bar1m['high']
    bar1m['longLow']=bar1m['low']
    bar1m['shortLow']=bar1m['low']
    bar1m['highshift1']=bar1m['high'].shift(1).fillna(0)
    bar1m['lowshift1']=bar1m['low'].shift(1).fillna(0)
    bar1m.loc[bar1m['open']<bar1m['close'],'longHigh']=bar1m['highshift1']
    bar1m.loc[bar1m['open']>bar1m['close'],'shortLow']=bar1m['lowshift1']
    timestart=datetime.datetime.now()
    dslCal(symbol, K_MIN, 'Set0 MS3 ML8 KN6 DN6', bar1m, barxm, pricetick, slip, -0.022, currentpath+'\\')
    timedsl=timestart-datetime.datetime.now()
    timestart=datetime.datetime.now()
    fastDslCal(symbol, K_MIN, 'Set0 MS3 ML8 KN6 DN6', bar1m, barxm, pricetick, slip, -0.022, currentpath + '\\')
    timefast=timestart-datetime.datetime.now()
    print "time dsl cost:",timedsl
    print "time fast cost:",timefast
    print 'fast delta:',timefast-timedsl