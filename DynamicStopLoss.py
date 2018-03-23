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
        data1m = bar1m.loc[(bar1m['utc_time'] > startutc) & (bar1m['utc_time'] < endutc)]
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
    #参数配置
    exchange_id = 'SHFE'
    sec_id='RB'
    symbol = '.'.join([exchange_id, sec_id])
    K_MIN = 600
    topN=5000
    pricetick=DC.getPriceTick(symbol)
    slip=pricetick
    starttime='2017-09-01'
    endtime='2017-12-11'
    tickstarttime='2017-10-01'
    tickendtime='2017-12-01'
    #优化参数
    stoplossStep=-0.002
    #stoplossList = np.arange(-0.022, -0.042, stoplossStep)
    stoplossList=[-0.022]
    #文件路径
    upperpath=DC.getUpperPath(uppernume=2)
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN)])
    oprresultpath=resultpath+foldername

    # 读取finalresult文件并排序，取前topN个
    finalresult=pd.read_csv(oprresultpath+"\\"+symbol+str(K_MIN)+" finanlresults.csv")
    finalresult=finalresult.sort_values(by='end_cash',ascending=False)
    totalnum=finalresult.shape[0]

    #原始数据处理

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

    tickdatasupplier=DC.TickDataSupplier(symbol,tickstarttime,tickendtime)

    os.chdir(oprresultpath)
    allresultdf = pd.DataFrame(columns=['setname', 'slTarget','worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                                     'old_SR',
                                     'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                                     'maxSingleLoss', 'maxSingleDrawBack'])
    allnum=0
    for stoplossTarget in stoplossList:
        resultList = []
        dslFolderName="DynamicStopLoss" + str(stoplossTarget*1000)
        try:
            os.mkdir(dslFolderName)#创建文件夹
        except:
            print 'folder already exist'
        print ("stoplossTarget:%f"%stoplossTarget)

        for sn in range(0, topN):
            opr = finalresult.iloc[sn]
            setname=opr['Setname']
            dslCalRealTick(symbol, K_MIN, setname, tickdatasupplier,barxm,pricetick,slip,stoplossTarget, dslFolderName + '\\')
            #l=dslCal(symbol=symbol,K_MIN=K_MIN,setname=setname,bar1m=bar1m,barxm=barxm,pricetick=pricetick,slip=slip,slTarget=stoplossTarget,tofolder=dslFolderName+'\\')
            #resultList.append(l)
            #allresultlist.append(l)

        '''
        pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)
        l = []

        for sn in range(0,topN):
            opr = finalresult.iloc[sn]
            setname = opr['Setname']
            #l.append(pool.apply_async(dslCal,
            #                          (symbol, K_MIN, setname, bar1m,barxm,pricetick,slip,stoplossTarget, dslFolderName + '\\')))
            l.append(pool.apply_async(dslCalRealTick,
                                      (symbol, K_MIN, setname, tickdatasupplier,barxm,pricetick,slip,stoplossTarget, dslFolderName + '\\')))
        pool.close()
        pool.join()
        '''
        '''
        resultdf=pd.DataFrame(columns=['setname','slTarget','worknum','old_endcash','old_Annual','old_Sharpe','old_Drawback','old_SR',
                                                  'new_endcash','new_Annual','new_Sharpe','new_Drawback','new_SR','maxSingleLoss','maxSingleDrawBack'])
        i = 0
        for res in l:
            resultdf.loc[i]=res.get()
            #allresultdf.loc[allnum]=resultdf.loc[i]
            i+=1
            allnum+=1
        resultdf['cashDelta']=resultdf['new_endcash']-resultdf['old_endcash']
        resultdf.to_csv(dslFolderName+'\\'+symbol+str(K_MIN)+' finalresult_by_tick'+str(stoplossTarget)+'.csv')
        '''
    #allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    #allresultdf.to_csv(symbol + str(K_MIN) + ' finalresult_dsl_by_tick.csv')