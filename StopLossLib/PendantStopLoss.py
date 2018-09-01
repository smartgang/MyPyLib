# -*- coding: utf-8 -*-
"""
atr吊灯止损：持仓后回撤一定比例的atr则止损出场，atr取值为最近一根完整K线的atr
yoyo止损：持仓某一K线下跌超过一定比例的atr则止损出场，atr取值为最近一根完整K线的atr

atr吊灯是主要的止损方法，yoyo是为了防某持仓后突然向下的行情造成较大的亏损，这两个止损要同时应用。另外可以考虑再搭配ownl或者frsl使用。

代码关键点：
先使用传入的xmbar_dic计算tr和atr，再组合成主连的xmbar
使用预处理后的1m数据虚拟tick数据
将xmbar上的atr数据通过utc_endtime对齐映射到1m数据的utc_time上
因为1m使用的是上一根完整K线的atr,对齐后需要shift(1)，再按上一个非零值填充
"""
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import ResultStatistics as RS
import ATR
import math
import time

def bar1m_prepare(bar1m):
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


def get_long_stop_loss_by_tick(bar_df):
    bardf = pd.DataFrame({'high': bar_df['longHigh'], 'low': bar_df['longLow'], 'strtime': bar_df['strtime'], 'utc_time': bar_df['utc_time'],
                          'pendant_value': bar_df['pendant_value']})
    # 多仓止损
    bardf['max2here'] = bardf['high'].expanding().max()
    bardf['dd2here'] = bardf['max2here'] - bardf['low']
    bardf['pendant_dd'] = bardf['dd2here'] - bardf['pendant_value']     # 吊灯止损

    rows = bardf.loc[bardf['pendant_dd'] >= 0]
    if rows.shape[0] > 0:
        temp = rows.iloc[0]
        sl_price = temp['max2here'] - temp['pendant_value']  # 这个止损价格还需要再基于price tick做向上取整处理
        strtime = temp['strtime']
        utctime = temp['utc_time']
        sl_index = 0
        return sl_price, strtime, utctime, sl_index

    return 0, '', 0, 0


def get_short_stop_loss_by_tick(bar_df):
    bardf = pd.DataFrame({'high': bar_df['shortHigh'], 'low': bar_df['shortLow'], 'strtime': bar_df['strtime'], 'utc_time': bar_df['utc_time'],
                          'pendant_value':bar_df['pendant_value']})
    # 空仓止损
    bardf['min2here'] = bardf['low'].expanding().min()
    bardf['dd2here'] = bardf['high'] - bardf['min2here']
    bardf['pendant_dd'] = bardf['dd2here'] - bardf['pendant_value']  # 吊灯止损

    rows = bardf.loc[bardf['pendant_dd'] >= 0]
    if rows.shape[0] > 0:
        temp = rows.iloc[0]
        sl_price = temp['min2here'] + temp['pendant_value']  # 这个止损价格还需要再基于price tick做向上取整处理
        strtime = temp['strtime']
        utctime = temp['utc_time']
        sl_index = 0
        return sl_price, strtime, utctime, sl_index

    return 0, '', 0, 0


def pendant(strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, sl_para_dic, result_para_dic, tofolder, indexcols, timestart):
    #time_enter = time.time()
    #print ("time enter:%.4f" % (time_enter - timestart))
    atr_pendant_n = sl_para_dic['atr_pendant_n']
    atr_pendant_rate = sl_para_dic['atr_pendant_rate']
    para_name = sl_para_dic['para_name']

    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    symbol = symbolInfo.domain_symbol
    bt_folder = "%s %d backtesting\\" % (symbol, K_MIN)
    oprdf = pd.read_csv(bt_folder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(oprdf)
    for k, v in barxmdic.items():
        v['pendant_tr'], v['pendant_atr'] = ATR.ATR(v.high, v.low, v.close, atr_pendant_n)
        v['pendant_value'] = v['pendant_atr'] * atr_pendant_rate

    #time_atr = time.time()
    #print ("time atr:%.4f" % (time_atr - time_enter))
    bar1m = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), bar1mdic, symbolDomainDic)
    bar1m = bar1m_prepare(bar1m)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(), barxmdic, symbolDomainDic)
    #time_data = time.time()
    #print ("time data:%.4f" % (time_data - time_atr))
    # 把xm上的止损结果映射到1m上，后面直接在1m上计算止损
    barxm.set_index('utc_endtime', drop=False, inplace=True)
    bar1m.set_index('utc_endtime', drop=False, inplace=True)
    bar1m['pendant_value'] = barxm['pendant_value']
    bar1m['pendant_value'] = bar1m['pendant_value'].shift(1)
    bar1m.fillna(method='ffill', inplace=True)
    #time_data_map = time.time()
    #print ("time data map:%.4f" % (time_data_map - time_data))
    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprnum = oprdf.shape[0]

    price_tick = symbolInfo.getPriceTick()

    worknum = 0
    for i in range(oprnum):
        opr = oprdf.iloc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60  # 从开仓的xm线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime  # 一直到平仓的10m线结束
        oprtype = opr.tradetype
        data1m = bar1m.loc[(bar1m['utc_time'] >= startutc) & (bar1m['utc_time'] < endutc)]
        if oprtype == 1:
            price, strtime, utctime, timeindex, = get_long_stop_loss_by_tick(data1m)
            if price != 0:
                #fprice = math.floor(price)   # 多仓的止损价向下取整
                fprice = price//price_tick * price_tick
                oprdf.ix[i, 'new_closeprice'] = fprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1

        elif oprtype == -1:
            price, strtime, utctime, timeindex= get_short_stop_loss_by_tick(data1m)
            if price!= 0:
                #fprice = math.ceil(price)   # 空仓的止损价向上取整
                fprice = price//price_tick * price_tick + max(price_tick, price % price_tick)
                oprdf.ix[i, 'new_closeprice'] = fprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum += 1
        else:
            # 被去极值的操作，oprtype为0,不做止损操作
            pass
    #time_stl = time.time()
    #print ("time stl:%.4f" % (time_stl - time_data_map))
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

    #bar1m.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' bar1m.csv')
    #barxm.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' barxm.csv')
    # 保存新的result文档
    oprdf.to_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' result_pendant_by_tick.csv', index=False)
    olddailydf = pd.read_csv(bt_folder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv', index_col='date')
    # 计算统计结果
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)

    dailyK = DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult_pendant_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)
    #time_result = time.time()
    #print ("time result:%.4f" % (time_result - time_stl))
    del oprdf
    print newr
    return [setname, para_name, worknum] + oldr + newr

if __name__ == '__main__':
    pass
