# -*- coding: utf-8 -*-
"""
策略本身不带出场，全靠止盈止损出场，所以在止损取数时没有下限
从进场点开始，while True向下取数（还要判断是否达到原始数据下限），如果达到止损或者止盈点，就break出来
使用1min的high和low来模拟tick，1min数据不做阴阳线预处理，如果1min同时满足止盈和止损，则取止损作为结果
"""
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import os
import ResultStatistics as RS
import multiprocessing


def fix_value_stop_loss(strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, result_para_dic, spr, slr, tofolder, indexcols):
    print ("fix_value_stop_loss: setname:%s, spr%.1f slr%.1f" % (setname, spr, slr))
    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    symbol = symbolInfo.domain_symbol
    oprdf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(oprdf)
    bar1m = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), bar1mdic, symbolDomainDic)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), barxmdic, symbolDomainDic)
    #bar1m.set_index('utc_time', inplace=True)
    barxm.set_index('utc_time', inplace=True)

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprdf['max_opr_gain'] = 0  # 本次操作期间的最大收益
    oprdf['min_opr_gain'] = 0  # 本次操作期间的最小收益
    oprdf['max_dd'] = 0
    oprnum = oprdf.shape[0]

    pricetick = symbolInfo.getPriceTick()
    worknum = 0

    for i in range(oprnum):
        opr = oprdf.iloc[i]
        #startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60  # 从开仓的10m线结束后开始
        #endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime  # 一直到平仓的10m线结束
        openutc = opr.openutc
        openprice = opr.openprice
        startutc = barxm.loc[openutc].utc_endtime - 60

        #spv = barxm.iloc[openutc].ATR * spr
        #slv = barxm.iloc[openutc].ATR * slr

        spv = 5  # 固定取值
        slv = 8  # 固定取值

        oprtype = opr.tradetype
        openprice = opr.openprice
        start_index_1m = bar1m[bar1m['utc_time'].isin([startutc])].index[0]  # 开仓位置在1m数据中的index,要从下一根开始算止盈止损
        while True:
            start_index_1m += 1
            high_1m = bar1m.loc[start_index_1m,'high']
            low_1m = bar1m.loc[start_index_1m].low
            if oprtype == 1:
                if low_1m <= (openprice - slv):
                    # 最低值达到止损门限
                    oprdf.ix[i, 'new_closeprice'] = openprice - slv
                    oprdf.ix[i, 'new_closetime'] = bar1m.iloc[start_index_1m].strtime
                    oprdf.ix[i, 'new_closeindex'] = start_index_1m
                    oprdf.ix[i, 'new_closeutc'] = bar1m.iloc[start_index_1m].utc_time
                    break
                elif high_1m >= (openprice + spv):
                    # 最大值达到止盈门限
                    oprdf.ix[i, 'new_closeprice'] = openprice + spv
                    oprdf.ix[i, 'new_closetime'] = bar1m.iloc[start_index_1m].strtime
                    oprdf.ix[i, 'new_closeindex'] = start_index_1m
                    oprdf.ix[i, 'new_closeutc'] = bar1m.iloc[start_index_1m].utc_time
                    break

            elif oprtype == -1:
                if high_1m >= (openprice + slv):
                    # 最大值达到止损门限
                    oprdf.ix[i, 'new_closeprice'] = openprice + slv
                    oprdf.ix[i, 'new_closetime'] = bar1m.iloc[start_index_1m].strtime
                    oprdf.ix[i, 'new_closeindex'] = start_index_1m
                    oprdf.ix[i, 'new_closeutc'] = bar1m.iloc[start_index_1m].utc_time
                    break
                elif low_1m <= (openprice - spv):
                    # 最大值达到止盈门限
                    oprdf.ix[i, 'new_closeprice'] = openprice - spv
                    oprdf.ix[i, 'new_closetime'] = bar1m.iloc[start_index_1m].strtime
                    oprdf.ix[i, 'new_closeindex'] = start_index_1m
                    oprdf.ix[i, 'new_closeutc'] = bar1m.iloc[start_index_1m].utc_time
                    break
            else:
                # 被去极值的操作，oprtype为0,不做止损操作
                pass
    slip = symbolInfo.getSlip()
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']

    # 去极值：在parallel的去极值结果上，把极值的new_ret和new_ret_r值0
    if result_para_dic['remove_polar_switch']:
        oprdf.loc[oprdf['tradetype']==0, 'new_ret'] = 0
        oprdf.loc[oprdf['tradetype']==0, 'new_ret_r'] = 0

    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                                  symbolInfo,
                                                                                                                  initialCash,
                                                                                                                  positionRatio, ret_col='new_ret')
    # 保存新的result文档
    oprdf.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' resultDSL_by_tick.csv', index=False)
    olddailydf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv', index_col='date')
    # 计算统计结果
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)

    barxm.reset_index(drop=False, inplace=True)
    dailyK = DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresultDSL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)

    del oprdf
    # return [setname,slTarget,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate]
    return [setname, spr, slr, worknum] + oldr + newr

if __name__ == '__main__':
    import datetime

    # 参数配置
    exchange_id = 'SHFE'
    sec_id = 'RB'
    symbol = '.'.join([exchange_id, sec_id])
    K_MIN = 600
    topN = 5000
    pricetick = DC.getPriceTick(symbol)
    slip = pricetick
    starttime = '2016-01-01'
    endtime = '2018-03-31'
    # 优化参数
    stoplossStep = -0.002
    # stoplossList = np.arange(-0.022, -0.042, stoplossStep)
    stoplossList = [-0.022]
    # 文件路径

    currentpath = DC.getCurrentPath()
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
    timestart = datetime.datetime.now()
    dslCal(symbol, K_MIN, 'Set0 MS3 ML8 KN6 DN6', bar1m, barxm, pricetick, slip, -0.022, currentpath + '\\')
    timedsl = timestart - datetime.datetime.now()
    timestart = datetime.datetime.now()
    fastDslCal(symbol, K_MIN, 'Set0 MS3 ML8 KN6 DN6', bar1m, barxm, pricetick, slip, -0.022, currentpath + '\\')
    timefast = timestart - datetime.datetime.now()
    print "time dsl cost:", timedsl
    print "time fast cost:", timefast
    print 'fast delta:', timefast - timedsl
