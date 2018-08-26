# -*- coding: utf-8 -*-
"""
从实盘运作经验看，可以考虑将PT底线随着持仓时间不断抬升，以保护利润。总体思想就是持仓越久，保留越多的利润。
以ownl触发保护时所在的第1根xmbar开始，每1根xmbar提升一定数量的底线。
每次底线的提升量，可以有两种方式，一种是线性提升，一种是指数提升
线性提升即每次提升固定数值
指数提升即每次提升固定的比例
相比于原ownl，gownl止损策略的起始止损点可以更低
参数接口：
保护门限：gownl_protect
起始底线：gownl_floor
步动值：gownl_step
"""
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


def get_long_gownl_by_tick(bardf, openprice, gownl_protect, gownl_floor, gownl_step):
    df = pd.DataFrame({'high': bardf['longHigh'], 'low': bardf['longLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'], 'index_num':bardf['index_num']})
    df['max2here'] = df['high'].expanding().max()
    df['maxEarnRate'] = df['max2here'] / openprice - 1
    df2 = df.loc[df['maxEarnRate'] > gownl_protect]
    if df2.shape[0] > 0:
        protect_index_num = df2.iloc[0, 'index_num']
        df2['protect_time'] = df2['index_num'] - protect_index_num  # 计算出保护时长
        df2['protect_floor'] = openprice + gownl_floor + df2['protect_time'] * gownl_step
        tempdf = df2.loc[df2['low'] <= df2['protect_floor']]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            newcloseprice = temp['protect_floor']
            strtime = temp['strtime']
            utctime = temp['utc_time']
            newcloseindex = temp['index_num']
            return newcloseprice, strtime, utctime, newcloseindex
    return 0, ' ', 0, 0


def get_short_gownl_by_tick(bardf, openprice, gownl_protect, gownl_floor, gownl_step):
    df = pd.DataFrame({'high': bardf['shortHigh'], 'low': bardf['shortLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'], 'index_num':bardf['index_num']})
    df['min2here'] = df['low'].expanding().min()
    df['maxEarnRate'] = 1 - df['min2here'] / openprice
    df2 = df.loc[df['maxEarnRate'] > gownl_protect]
    if df2.shape[0] > 0:
        protect_index_num = df2.iloc[0, 'index_num']
        df2['protect_time'] = df2['index_num'] - protect_index_num  # 计算出保护时长
        df2['protect_floor'] = openprice - gownl_floor - df2['protect_time'] * gownl_step
        tempdf = df2.loc[df2['high'] >= df2['protect_floor']]
        if tempdf.shape[0] > 0:
            temp = tempdf.iloc[0]
            newcloseprice = temp['protect_floor']
            strtime = temp['strtime']
            utctime = temp['utc_time']
            timeindex = temp['index_num']
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
def gownl(strategy_name, symbol_info, bar_type, setname, bar1mdic, barxmdic, slt_para, result_para_dic, tofolder, indexcols, time_start):
    gownl_protect = slt_para['gownl_protect']
    gownl_floor = slt_para['gownl_floor']
    gownl_step = slt_para['gownl_step']
    gownl_target_name = "gownl_protect:%.3f, gownl_floor:%.1f, gownl_step: %.1f" % ( gownl_protect, gownl_floor, gownl_step)
    print ("setname:%s %s" % (setname, gownl_target_name))
    symbol = symbol_info.domain_symbol
    bt_folder = "%s %d backtesting\\" % (symbol, bar_type)
    oprdf = pd.read_csv(bt_folder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbol_info.amendSymbolDomainDicByOpr(oprdf)
    bar1m = DC.getDomainbarByDomainSymbol(symbol_info.getSymbolList(), bar1mdic, symbolDomainDic)
    bar1m = bar1mPrepare(bar1m)
    barxm = DC.getDomainbarByDomainSymbol(symbol_info.getSymbolList(), barxmdic, symbolDomainDic)

    barxm['index_num'] = range(barxm.shape[0])

    barxm.set_index('utc_time', drop=False, inplace=True)   # 开始时间对齐
    bar1m.set_index('utc_time', drop=False, inplace=True)
    bar1m['index_num'] = barxm['index_num']
    bar1m.fillna(method='ffill', inplace=True)  # 用上一个非0值来填充

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
        startutc = barxm.loc[opr['openutc'], 'utc_endtime'] - 60  # 从开仓的10m线结束后开始
        endutc = barxm.loc[opr['closeutc'], 'utc_endtime']  # 一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        data1m = bar1m.loc[startutc:endutc]
        if oprtype == 1:
            newcloseprice, strtime, utctime, timeindex = get_long_gownl_by_tick(data1m, openprice, gownl_protect, gownl_floor, gownl_step)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

        else:
            newcloseprice, strtime, utctime, timeindex = get_short_gownl_by_tick(data1m, openprice, gownl_protect, gownl_floor, gownl_step)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

    slip = symbol_info.getSlip()
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                                  symbol_info,
                                                                                                                  initialCash,
                                                                                                                  positionRatio, ret_col='new_ret')
    # 保存新的result文档
    oprdf.to_csv(tofolder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' resultGOWNL_by_tick.csv', index=False)

    olddailydf = pd.read_csv(bt_folder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' dailyresult.csv', index_col='date')
    # 计算统计结果
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)

    dailyK = DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbol_info, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' dailyresultGOWNL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)
    print newr
    return [setname, gownl_target_name, worknum] + oldr + newr
    # return [setname,winSwitch,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate]


# ================================================================================================
def progress_gownl(strategy_name, symbol_info, bar_type, setname, bar1mdic, barxmdic, slt_para, result_para_dic, tofolder, indexcols, time_start):
    """
    增量式止损
    """
    print ("ownl_target:%.3f, nolossThreshhold;%d,setname:%s" % (winSwitch, nolossThreshhold, setname))
    symbol = symbol_info.domain_symbol
    bt_folder = "%s %d backtesting\\" % (symbol, bar_type)
    orioprdf = pd.read_csv(bt_folder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' result.csv')

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
    pass
