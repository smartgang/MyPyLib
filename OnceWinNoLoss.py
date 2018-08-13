# -*- coding: utf-8 -*-
'''
有赚就不亏止损策略
按tick级别判断，盈利超过千3后，回撤达到3个pricetick的保护价就平仓
tick模拟：1min的high和low模拟1min内tick的最高和最低价
'''
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import os
import ResultStatistics as RS
import multiprocessing


def bar1mPrepare(bar1m):
    bar1m['longHigh'] = bar1m['high']
    bar1m['shortHigh'] = bar1m['high']
    bar1m['longLow'] = bar1m['low']
    bar1m['shortLow'] = bar1m['low']
    bar1m['highshift1'] = bar1m['high'].shift(1).fillna(0)
    bar1m['lowshift1'] = bar1m['low'].shift(1).fillna(0)
    bar1m.loc[bar1m['open'] < bar1m['close'], 'longHigh'] = bar1m['highshift1']
    bar1m.loc[bar1m['open'] > bar1m['close'], 'shortLow'] = bar1m['lowshift1']
    bar1m.drop('highshift1', axis=1, inplace=True)
    bar1m.drop('lowshift1', axis=1, inplace=True)
    # bar1m['Unnamed: 0'] = range(bar1m.shape[0])

    return bar1m


def getLongNoLossByTick(bardf, openprice, winSwitch, nolossThreshhold):
    '''
    1.计算截至当前的最大盈利：maxEarnRate:expanding().max()/openprice
    2.取maxEarnRate大于winSwitch的数据，如果数据量大于0，表示触发了保护门限
    3.超过保护的数据中，取第1个价格小于或等于openprice+nolossThreshhold的，即为触发平仓时机
    4.返回平仓的参数：new_closeprice=openprice+nolossThreshhold,new_closeutc,new_closeindex,new_closetime
    '''
    df = pd.DataFrame({'high': bardf['longHigh'], 'low': bardf['longLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time']})
    df['max2here'] = df['high'].expanding().max()
    df['maxEarnRate'] = df['max2here'] / openprice - 1
    df2 = df.loc[df['maxEarnRate'] > winSwitch]
    if df2.shape[0] > 0:
        tempdf = df2.loc[df2['low'] <= (openprice + nolossThreshhold)]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            newcloseprice = openprice + nolossThreshhold
            strtime = temp['strtime']
            utctime = temp['utc_time']
            return newcloseprice, strtime, utctime, 0
    return 0, ' ', 0, 0


def getShortNoLossByTick(bardf, openprice, winSwitch, nolossThreshhold):
    '''
    1.计算截至当前的最大盈利：maxEarnRate:expanding().max()/openprice
    2.取maxEarnRate大于winSwitch的数据，如果数据量大于0，表示触发了保护门限
    3.超过保护的数据中，取第1个价格小于或等于openprice+nolossThreshhold的，即为触发平仓时机
    4.返回平仓的参数：new_closeprice=openprice+nolossThreshhold,new_closeutc,new_closeindex,new_closetime
    '''
    df = pd.DataFrame({'high': bardf['shortHigh'], 'low': bardf['shortLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time']})
    df['min2here'] = df['low'].expanding().min()
    df['maxEarnRate'] = 1 - df['min2here'] / openprice
    df2 = df.loc[df['maxEarnRate'] > winSwitch]
    if df2.shape[0] > 0:
        tempdf = df2.loc[df2['high'] >= (openprice - nolossThreshhold)]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            newcloseprice = openprice - nolossThreshhold
            strtime = temp['strtime']
            utctime = temp['utc_time']
            timeindex = 0
            return newcloseprice, strtime, utctime, timeindex
    return 0, ' ', 0, 0


# ==========================================================================================================
def getLongNoLossByRealtick(tickdf, openprice, winSwitch, nolossThreshhold):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time']})
    df['max2here'] = df['close'].expanding().max()
    df['maxEarnRate'] = df['max2here'] / openprice - 1
    df2 = df.loc[df['maxEarnRate'] > winSwitch]
    if df2.shape[0] > 0:
        tempdf = df2.loc[df2['close'] <= (openprice + nolossThreshhold)]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            newcloseprice = temp['close']
            strtime = temp['strtime']
            utctime = temp['utc_time']
            timeindex = 0
            return newcloseprice, strtime, utctime, timeindex
    return 0, ' ', 0, 0


def getShortNoLossByRealtick(tickdf, openprice, winSwitch, nolossThreshhold):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time']})
    df['min2here'] = df['close'].expanding().min()
    df['maxEarnRate'] = 1 - df['min2here'] / openprice
    df2 = df.loc[df['maxEarnRate'] > winSwitch]
    if df2.shape[0] > 0:
        tempdf = df2.loc[df2['close'] >= (openprice - nolossThreshhold)]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            # newcloseprice = openprice-nolossThreshhold
            newcloseprice = temp['close']
            strtime = temp['strtime']
            utctime = temp['utc_time']
            timeindex = 0
            return newcloseprice, strtime, utctime, timeindex
    return 0, ' ', 0, 0


def ownlCalRealTick(symbol, K_MIN, setname, ticksupplier, barxm, winSwitch, nolossThreshhold, slip, tofolder):
    print 'ownl;', str(winSwitch), ',setname:', setname
    oprdf = pd.read_csv(symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    tickstartutc, tickendutc = ticksupplier.getDateUtcRange()
    # 只截取tick时间范围内的opr
    oprdf = oprdf.loc[(oprdf['openutc'] > tickstartutc) & (oprdf['openutc'] < tickendutc)]

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    # oprnum = oprdf.shape[0]
    oprindex = oprdf.index.tolist()
    worknum = 0
    # for i in range(oprnum):
    for i in oprindex:
        opr = oprdf.loc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime  # 从开仓的10m线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime  # 一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        tickdata = ticksupplier.getTickDataByUtc(startutc, endutc)
        if oprtype == 1:
            newcloseprice, strtime, utctime, timeindex = getLongNoLossByRealtick(tickdata, openprice, winSwitch, nolossThreshhold)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

        else:
            newcloseprice, strtime, utctime, timeindex = getShortNoLossByRealtick(tickdata, openprice, winSwitch, nolossThreshhold)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['retdelta'] = oprdf['new_ret'] - oprdf['ret']
    oprdf.to_csv(tofolder + symbol + str(K_MIN) + ' ' + setname + ' resultOWNL_by_realtick.csv')


# ================================================================================================
def ownlCal(strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, winSwitch, nolossThreshhold, result_para_dic, tofolder, indexcols):
    print ("ownl_target:%.3f, nolossThreshhold;%d,setname:%s" % (winSwitch, nolossThreshhold, setname))
    symbol = symbolInfo.domain_symbol
    oprdf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(oprdf)
    bar1m = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), bar1mdic, symbolDomainDic)
    bar1m = bar1mPrepare(bar1m)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), barxmdic, symbolDomainDic)

    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprnum = oprdf.shape[0]
    worknum = 0
    for i in range(oprnum):
        opr = oprdf.iloc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60  # 从开仓的10m线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime  # 一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        data1m = bar1m.loc[(bar1m['utc_time'] >= startutc) & (bar1m['utc_time'] < endutc)]
        if oprtype == 1:
            newcloseprice, strtime, utctime, timeindex = getLongNoLossByTick(data1m, openprice, winSwitch, nolossThreshhold)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

        else:
            newcloseprice, strtime, utctime, timeindex = getShortNoLossByTick(data1m, openprice, winSwitch, nolossThreshhold)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

    slip = symbolInfo.getSlip()
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                                  symbolInfo,
                                                                                                                  initialCash,
                                                                                                                  positionRatio, ret_col='new_ret')
    # 保存新的result文档
    oprdf.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' resultOWNL_by_tick.csv', index=False)

    olddailydf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv', index_col='date')
    # 计算统计结果
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)

    dailyK = DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresultOWNL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)
    '''
    oldendcash = oprdf['own cash'].iloc[-1]
    oldAnnual = RS.annual_return(oprdf)
    oldSharpe = RS.sharpe_ratio(oprdf)
    oldDrawBack = RS.max_drawback(oprdf)[0]
    oldSR = RS.success_rate(oprdf)
    newendcash = oprdf['new_own cash'].iloc[-1]
    newAnnual = RS.annual_return(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc')
    newSharpe = RS.sharpe_ratio(oprdf,cash_col='new_own cash',closeutc_col='new_closeutc',retr_col='new_ret_r')
    newDrawBack = RS.max_drawback(oprdf,cash_col='new_own cash')[0]
    newSR = RS.success_rate(oprdf,ret_col='new_ret')
    max_single_loss_rate = abs(oprdf['new_ret_r'].min())
    #max_retrace_rate = oprdf['new_retrace rate'].max()
    '''
    print newr
    return [setname, winSwitch, worknum] + oldr + newr
    # return [setname,winSwitch,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate]


# ================================================================================================
def progressOwnlCal(strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, winSwitch, nolossThreshhold, result_para_dic, tofolder, indexcols):
    """
    增量式止损
    """
    print ("ownl_target:%.3f, nolossThreshhold;%d,setname:%s" % (winSwitch, nolossThreshhold, setname))
    symbol = symbolInfo.domain_symbol
    orioprdf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(orioprdf)
    bar1m = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), bar1mdic, symbolDomainDic)
    bar1m = bar1mPrepare(bar1m)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), barxmdic, symbolDomainDic)

    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    orioprnum = orioprdf.shape[0]
    ownldf = pd.read_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' resultOWNL_by_tick.csv')
    # ownldf.drop('Unnamed: 0.1',axis=1,inplace=True)
    ownloprnum = ownldf.shape[0]
    oprdf = ownldf
    if orioprnum > ownloprnum:
        oprdf = orioprdf.loc[ownloprnum:, :]
        oprdf['new_closeprice'] = oprdf['closeprice']
        oprdf['new_closetime'] = oprdf['closetime']
        oprdf['new_closeindex'] = oprdf['closeindex']
        oprdf['new_closeutc'] = oprdf['closeutc']
        oprdf['max_opr_gain'] = 0  # 本次操作期间的最大收益
        oprdf['min_opr_gain'] = 0  # 本次操作期间的最小收益
        oprdf['max_dd'] = 0
        for i in range(ownloprnum, orioprnum):
            opr = oprdf.loc[i]
            startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60  # 从开仓的10m线结束后开始
            endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime  # 一直到平仓的10m线结束
            oprtype = opr.tradetype
            openprice = opr.openprice
            data1m = bar1m.loc[(bar1m['utc_time'] >= startutc) & (bar1m['utc_time'] < endutc)]
            if oprtype == 1:
                newcloseprice, strtime, utctime, timeindex = getLongNoLossByTick(data1m, openprice, winSwitch, nolossThreshhold)
                if newcloseprice != 0:
                    oprdf.ix[i, 'new_closeprice'] = newcloseprice
                    oprdf.ix[i, 'new_closetime'] = strtime
                    oprdf.ix[i, 'new_closeindex'] = timeindex
                    oprdf.ix[i, 'new_closeutc'] = utctime
            else:
                newcloseprice, strtime, utctime, timeindex = getShortNoLossByTick(data1m, openprice, winSwitch, nolossThreshhold)
                if newcloseprice != 0:
                    oprdf.ix[i, 'new_closeprice'] = newcloseprice
                    oprdf.ix[i, 'new_closetime'] = strtime
                    oprdf.ix[i, 'new_closeindex'] = timeindex
                    oprdf.ix[i, 'new_closeutc'] = utctime

        slip = symbolInfo.getSlip()
        # 2017-12-08:加入滑点
        oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
        oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
        oprdf['new_commission_fee'] = 0
        oprdf['new_per earn'] = 0
        oprdf['new_own cash'] = 0
        oprdf['new_hands'] = 0
        oprdf = pd.concat([ownldf, oprdf])
        oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                                      symbolInfo,
                                                                                                                      initialCash,
                                                                                                                      positionRatio, ret_col='new_ret')
        # 保存新的result文档
        oprdf.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' resultOWNL_by_tick.csv', index=False)

    # 计算统计结果
    worknum = oprdf.loc[oprdf['new_closeindex'] != oprdf['closeindex']].shape[0]
    olddailydf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv', index_col='date')
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)

    dailyK = DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresultOWNL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)

    del oprdf
    del orioprdf
    del ownldf
    print newr
    return [setname, winSwitch, worknum] + oldr + newr


if __name__ == '__main__':
    # 参数配置
    exchange_id = 'SHFE'
    sec_id = 'RB'
    symbol = '.'.join([exchange_id, sec_id])
    K_MIN = 600
    topN = 5000
    pricetick = DC.getPriceTick(symbol)
    slip = pricetick
    starttime = '2017-09-01'
    endtime = '2017-12-11'
    tickstarttime = '2017-10-01'
    tickendtime = '2017-12-01'
    # 优化参数
    stoplossStep = 0.001
    # winSwitchList = np.arange(0.003, 0.011, stoplossStep)
    winSwitchList = [0.009]
    nolossThreshhold = 3 * pricetick

    # 文件路径
    upperpath = DC.getUpperPath(uppernume=2)
    resultpath = upperpath + "\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN)])
    oprresultpath = resultpath + foldername

    # 读取finalresult文件并排序，取前topN个
    finalresult = pd.read_csv(oprresultpath + "\\" + symbol + str(K_MIN) + " finanlresults.csv")
    finalresult = finalresult.sort_values(by='end_cash', ascending=False)
    totalnum = finalresult.shape[0]

    # 原始数据处理
    bar1m = DC.getBarData(symbol=symbol, K_MIN=60, starttime=starttime + ' 00:00:00', endtime=endtime + ' 00:00:00')
    barxm = DC.getBarData(symbol=symbol, K_MIN=K_MIN, starttime=starttime + ' 00:00:00', endtime=endtime + ' 00:00:00')
    # bar1m计算longHigh,longLow,shortHigh,shortLow
    bar1m['longHigh'] = bar1m['high']
    bar1m['shortHigh'] = bar1m['high']
    bar1m['longLow'] = bar1m['low']
    bar1m['shortLow'] = bar1m['low']
    bar1m['highshift1'] = bar1m['high'].shift(1).fillna(0)
    bar1m['lowshift1'] = bar1m['low'].shift(1).fillna(0)
    bar1m.loc[bar1m['open'] < bar1m['close'], 'longHigh'] = bar1m['highshift1']
    bar1m.loc[bar1m['open'] > bar1m['close'], 'shortLow'] = bar1m['lowshift1']

    tickdatasupplier = DC.TickDataSupplier(symbol, tickstarttime, tickendtime)

    os.chdir(oprresultpath)
    allresultdf = pd.DataFrame(columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                                        'old_SR',
                                        'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                                        'maxSingleLoss', 'maxSingleDrawBack'])
    for winSwitch in winSwitchList:
        resultList = []
        ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000)
        try:
            os.mkdir(ownlFolderName)  # 创建文件夹
        except:
            print "dir already exist!"
        print ("OnceWinNoLoss WinSwitch:%f" % winSwitch)

        # 顺序执行
        for sn in range(0, topN):
            opr = finalresult.iloc[sn]
            setname = opr['Setname']
            ownlCalRealTick(symbol, K_MIN, setname, tickdatasupplier, barxm, winSwitch, nolossThreshhold, slip, ownlFolderName + '\\')

        '''
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        l = []

        for sn in range(0,topN):
            opr = finalresult.iloc[sn]
            setname = opr['Setname']
            l.append(pool.apply_async(ownlCal,
                                      (symbol,K_MIN,setname,bar1m,barxm,winSwitch,nolossThreshhold,slip,ownlFolderName + '\\')))
        pool.close()
        pool.join()

        resultdf=pd.DataFrame(columns=['setname','winSwitch','worknum','old_endcash','old_Annual','old_Sharpe','old_Drawback','old_SR',
                                                  'new_endcash','new_Annual','new_Sharpe','new_Drawback','new_SR','maxSingleLoss','maxSingleDrawBack'])
        i = 0
        for res in l:
            resultdf.loc[i]=res.get()
            allresultdf.loc[allnum]=resultdf.loc[i]
            i+=1
            allnum+=1
        resultdf['cashDelta']=resultdf['new_endcash']-resultdf['old_endcash']
        resultdf.to_csv(ownlFolderName+'\\'+symbol+str(K_MIN)+' finalresult_by_tick'+str(winSwitch)+'.csv')
        '''
    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    # allresultdf.to_csv(symbol + str(K_MIN) + ' finalresult_ownl_by_tick.csv')
