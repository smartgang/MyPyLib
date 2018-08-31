# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import os
import ResultStatistics as RS
import multiprocessing
import AtrStopLoss
import GOWNL
import DynamicStopLoss
import OnceWinNoLoss
import FixRateStopLoss
import FixValueStopLoss

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

def get_atrsl_close_result(opr, bar_df, para_dic, price_tick):
    print ("atrsl not supported now!")
    return 0, ' ', 0, 0

def get_dsl_close_result(opr, bar_df, para_dic,  price_tick):
    opr_type = opr['tradetype']
    dsl_target = para_dic['dsl_target']
    if opr_type == 1:
        #return DynamicStopLoss.getLongDrawbackByTick(bar_df, dsl_target)
        max_dd, max_dd_close, maxprice, strtime, utctime, close_index = DynamicStopLoss.getLongDrawbackByTick(bar_df, dsl_target)
        if maxprice > 0:
            pprice = maxprice * (1+ dsl_target)
            close_price = pprice // price_tick * price_tick
        else:
            close_price = 0
        return close_price, strtime, utctime, close_index
    else:
        #return DynamicStopLoss.getShortDrawbackByTick(bar_df, dsl_target)
        max_dd, max_dd_close, maxprice, strtime, utctime, close_index = DynamicStopLoss.getLongDrawbackByTick(bar_df, dsl_target)
        if maxprice > 0:
            pprice = maxprice * (1 - dsl_target)
            close_price = pprice // price_tick * price_tick + max(price_tick, pprice % price_tick)
        else:
            close_price = 0
        return close_price, strtime, utctime, close_index

def get_ownl_close_result(opr, bar_df, para_dic,  price_tick):
    open_price = opr['openprice']
    opr_type = opr['tradetype']
    ownl_protect  = para_dic['ownl_protect']
    ownl_floor = para_dic['ownl_floor']
    if opr_type == 1:
        return OnceWinNoLoss.getLongNoLossByTick(bar_df, open_price, ownl_protect, ownl_floor)
    else:
        return OnceWinNoLoss.getShortNoLossByTick(bar_df, open_price, ownl_protect, ownl_floor)


def get_gownl_close_result(opr, bar_df, para_dic,  price_tick):
    opr_type = opr['tradetype']
    open_price = opr['openprice']
    gownl_protect = para_dic['gownl_protect']
    gownl_floor = para_dic['gownl_floor']
    gownl_step = para_dic['gownl_step']
    if opr_type == 1:
        return GOWNL.get_long_gownl_by_tick(bar_df, open_price, gownl_protect, gownl_floor, gownl_step)
    else:
        return GOWNL.get_short_gownl_by_tick(bar_df, open_price, gownl_protect, gownl_floor, gownl_step)


def get_frsl_close_result(opr, bar_df, para_dic, price_tick):
    fixRate = para_dic['frsl_target']
    opr_type = opr['tradetype']
    open_price = opr['openprice']
    if opr_type == 1:
        return FixRateStopLoss.getLongFixRateLossByTick(bar_df, open_price, fixRate)
    else:
        return FixRateStopLoss.getShortFixRateLossByTick(bar_df, open_price, fixRate)

def get_fvsl_close_result(opr, bar_df, para_dic,  price_tick):
    print ("fvsl not supported now!")
    return 0, ' ', 0, 0


close_function_map = {
    'dsl':get_dsl_close_result,
    'ownl':get_ownl_close_result,
    'gownl':get_gownl_close_result,
    'atrsl':get_atrsl_close_result,
    'frsl':get_frsl_close_result,
    'fvsl':get_fvsl_close_result
}

# ================================================================================================
def all_close(strategy_name, symbol_info, bar_type, setname, bar1mdic, barxmdic, all_close_para_list, result_para_dic, indexcols, time_start):
    symbol = symbol_info.domain_symbol
    price_tick = symbol_info.getPriceTick()
    bt_folder = "%s %d backtesting\\" % (symbol, bar_type)
    oprdf = pd.read_csv(bt_folder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' result.csv')

    close_type_list = []
    all_final_result_dic = {}  # 这个是用来保存每个文件的RS结果，返回给外部调用的
    all_close_result_dic = {}  # 这个是用来保存每个参数每次操作的止损结果
    for close_para in all_close_para_list:
        close_type_list.append(close_para['name'])
        final_result_dic = {}
        for para in close_para['paralist']:
            final_result_dic[para['para_name']] = []
            all_close_result_dic[para['para_name']] = []
        all_final_result_dic[close_para['name']] = final_result_dic

    symbolDomainDic = symbol_info.amendSymbolDomainDicByOpr(oprdf)
    bar1m = DC.getDomainbarByDomainSymbol(symbol_info.getSymbolList(), bar1mdic, symbolDomainDic)
    bar1m = bar1mPrepare(bar1m)
    barxm = DC.getDomainbarByDomainSymbol(symbol_info.getSymbolList(), barxmdic, symbolDomainDic)
    
    barxm.set_index('utc_time', drop=False, inplace=True)   # 开始时间对齐
    bar1m.set_index('utc_time', drop=False, inplace=True)
    if 'gownl' in close_type_list:
        # gownl数据预处理
        barxm['index_num'] = range(barxm.shape[0])
        bar1m['index_num'] = barxm['index_num']
        bar1m.fillna(method='ffill', inplace=True)  # 用上一个非0值来填充

    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    oprnum = oprdf.shape[0]
    worknum = 0
    for i in range(oprnum):
        opr = oprdf.iloc[i]
        startutc = barxm.loc[opr['openutc'], 'utc_endtime'] - 60  # 从开仓的10m线结束后开始
        endutc = barxm.loc[opr['closeutc'], 'utc_endtime']  # 一直到平仓的10m线结束
        data1m = bar1m.loc[startutc:endutc]
        for close_type_para in all_close_para_list:
            close_type = close_type_para['name']
            close_function = close_function_map[close_type]
            close_para_list = close_type_para['paralist']
            for close_para in close_para_list:
                newcloseprice, strtime, utctime, timeindex = close_function(opr, data1m, close_para, price_tick)
                all_close_result_dic[close_para['para_name']].append({
                    'new_closeprice':newcloseprice,
                    'new_closetime': strtime,
                    'new_closeutc': utctime,
                    'new_closeindex': timeindex
                })

    slip = symbol_info.getSlip()

    olddailydf = pd.read_csv(bt_folder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' dailyresult.csv', index_col='date')
    oldr = RS.getStatisticsResult(oprdf, False, indexcols, olddailydf)
    dailyK = DC.generatDailyClose(barxm)

    # 全部止损完后，针对每个止损参数要单独计算一次结果
    for close_type_para in all_close_para_list:
        close_type = close_type_para['name']
        folder_prefix = close_type_para['folderPrefix']
        file_suffix = close_type_para['fileSuffix']
        close_para_list = close_type_para['paralist']
        for close_para in close_para_list:
            para_name = close_para['para_name']
            close_result_list = all_close_result_dic[para_name]
            result_df = pd.DataFrame(close_para_list)
            oprdf_temp = pd.concat([oprdf, result_df], axis=1)
            oprdf_temp['new_ret'] = ((oprdf_temp['new_closeprice'] - oprdf_temp['openprice']) * oprdf_temp['tradetype']) - slip
            oprdf_temp['new_ret_r'] = oprdf_temp['new_ret'] / oprdf_temp['openprice']
            oprdf_temp['new_commission_fee'], oprdf_temp['new_per earn'], oprdf_temp['new_own cash'], oprdf_temp['new_hands'] = RS.calcResult(oprdf_temp,
                                                                                                                          symbol_info,
                                                                                                                          initialCash,
                                                                                                                          positionRatio, ret_col='new_ret')
            # 保存新的result文档
            tofolder = "%s%s\\" % (folder_prefix, para_name)
            oprdf_temp.to_csv(tofolder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' '+ file_suffix, index=False)


            dR = RS.dailyReturn(symbol_info, oprdf_temp, dailyK, initialCash)  # 计算生成每日结果
            dR.calDailyResult()
            dR.dailyClose.to_csv((tofolder + strategy_name + ' ' + symbol + str(bar_type) + ' ' + setname + ' daily'+file_suffix))
            newr = RS.getStatisticsResult(oprdf, True, indexcols, dR.dailyClose)
            final_result_dic = all_final_result_dic[close_type]
            final_result_dic[para_name] = [setname, para_name, worknum] + oldr + newr

    return all_final_result_dic
    # return [setname,winSwitch,worknum,oldendcash,oldAnnual,oldSharpe,oldDrawBack,oldSR,newendcash,newAnnual,newSharpe,newDrawBack,newSR,max_single_loss_rate]

if __name__ == '__main__':
    pass
