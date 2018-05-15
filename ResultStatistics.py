# -*- coding: utf-8 -*-
'''
策略结果统计：
    年化收益
    最大回撤
    平均涨幅
    上涨概率——没算
    最大连续上涨/下跌天数（次数）
    最次操作最大涨幅和最大跌幅
    收益波动率
    Beta——没有测试
    alpha——暂时未算
    Sharpe_ratio
    信息比率(IR)——需要转换为日收益，没有算
    累计收益率
    成功率
'''
import pandas as pd
from pandas import Series
from datetime import date
import numpy as np

def annual_return(resultdf,cash_col='own cash',closeutc_col='closeutc',openutc_col='openutc'):
    '''
    计算年化收益
    :param resultdf:包含所有操作结果
    公式：（账户最终价值/账户初始价值）^（250/回测期间总天数）-1
    :return:annual_return
    '''
    oprnum=resultdf.shape[0]
    startcash=resultdf.ix[0,cash_col]
    #startcash=20000
    startdate=date.fromtimestamp(resultdf.ix[0,openutc_col])
    endcash=resultdf.ix[oprnum-1,cash_col]
    enddate=date.fromtimestamp(resultdf.ix[oprnum-1,closeutc_col])
    datenum=float((enddate-startdate).days)+1
    return pow(endcash/startcash,250/datenum)-1

def max_drawback(resultdf,cash_col='own cash',opentime_col='opentime'):
    '''
    最大回撤
    :param resultdf:
    公式：最大回撤就是从一个高点到一个低点最大的下跌幅度
    :return:
    '''
    df=pd.DataFrame({'date':resultdf[opentime_col],'capital':resultdf[cash_col]})

    #df['max2here']=pd.expanding_max(df['capital'])
    df['max2here']=df['capital'].expanding().max()
    df['dd2here']=df['capital']/df['max2here']-1

    temp= df.sort_values(by='dd2here').iloc[0][['date','dd2here']]
    max_dd=temp['dd2here']
    end_date=temp['date']

    df=df[df['date']<=end_date]
    start_date=df.sort_values(by='capital',ascending=False).iloc[0]['date']
    #print('最大回撤为：%f,开始时间：%s,结果时间:%s'% (max_dd,start_date,end_date))
    return max_dd,start_date,end_date

def average_change(resultdf,retr_col='ret_r'):
    '''
    平均涨幅
    :param resultdf:
    :return:账户收益的平均值
    '''
    average=resultdf[retr_col].mean()
    return average

def max_successive_up(resultdf,ret_col='ret'):
    '''
    最大连续上涨和下跌的次数
    :param resultdf:
    :return:
    '''
    ret = resultdf[ret_col].tolist()
    max_successive_up=0
    max_successive_down=0
    r0 = ret[0]
    positivenum = int(0)
    negativenum = int(0)
    for r in ret:
        if r > 0:
            # 当前为正，判断之前的数
            if r0 > 0:
                # 如果当前为正，之前也为正，则正数+1
                positivenum += 1
            elif r0 <= 0:
                # 如果当正，之前为负，正数+1，负数保存并清0
                positivenum += 1
                max_successive_down=max(max_successive_down,negativenum)
                negativenum = 0
        elif r <= 0:
            if r0 > 0:
                # 如果当前为负，之前为正，则正数清并保存，负数+1
                negativenum += 1
                max_successive_up=max(max_successive_up,positivenum)
                positivenum = 0
            elif r0 <= 0:
                negativenum += 1
        r0 = r
    return max_successive_up,max_successive_down

def max_period_return(resultdf,retr_col='ret_r'):
    '''
    单次最大收益率和最大亏损率
    :param resultdf:
    :return:
    '''
    max_return=resultdf[retr_col].max()
    min_return=resultdf[retr_col].min()
    return max_return,min_return

def volatility(resultdf,retr_col='ret_r'):
    '''
    计算收益波动率:账户日收益的年化标准差
    :param resultdf:
    :return:
    '''
    from math import sqrt
    vol = resultdf[retr_col].std() * sqrt(250)
    return vol

def beta(resultdf,benchmart_rtn):
    '''
    计算beta系数!!未测试！！账户日收益与参考基准日收益的协方差 / 参考基准日收益的方差
    :param resultdf:
    :param benchmart_rtn:参考收益
    :return:
    '''
    b=resultdf.ret_r.cov(benchmart_rtn)/benchmart_rtn.var()
    return b

def sharpe_ratio(resultdf,cash_col='own cash',closeutc_col='closeutc',retr_col='ret_r',openutc_col='openutc'):
    '''
    计算夏普比率:（账户年化收益率-无风险利率）/ 收益波动率。
    :param resultdf:
    :return:
    '''
    #10年期国债年化收益率
    rf=0.0284

    #计算年化收益
    oprnum=resultdf.shape[0]
    startcash=resultdf.ix[0,cash_col]
    #startcash=20000
    startdate=date.fromtimestamp(resultdf.ix[0,openutc_col])
    endcash=resultdf.ix[oprnum-1,cash_col]
    enddate=date.fromtimestamp(resultdf.ix[oprnum-1,closeutc_col])
    datenum=float((enddate-startdate).days)+1
    annual_return = pow(endcash/startcash,250/datenum)-1

    #计算波动率
    from math import sqrt
    vol = resultdf[retr_col].std() * sqrt(250)

    #计算夏普比率
    sharpe=(annual_return-rf)/vol
    return sharpe

def info_ratio(resultdf,indexreturn):
    '''
    计算信息比率（账户日收益 - 参考基准日收益）的年化均值/年化标准差。
    :param resultdf:
    :param indexreturn:
    :return:
    '''
    pass

def success_rate(resultdf,ret_col='ret'):
    '''
    计算所有操作的成功率，ret>0为成功
    :param resultdf:
    :return:
    '''
    successcount=resultdf.loc[resultdf[ret_col]>0].shape[0]
    totalcount=resultdf.shape[0]
    return successcount/float(totalcount)

#===================================重新包装===============================
def opr_times(resultdf,new=False):
    '''操作次数'''
    return resultdf.shape[0]

def long_opr_times(resultdf,new=False):
    '''多操作次数'''
    return resultdf.loc[resultdf['tradetype']==1].shape[0]

def short_opr_times(resultdf,new=False):
    '''空操作次数'''
    return resultdf.loc[resultdf['tradetype']==-1].shape[0]

def end_cash(resultdf,new=False):
    '''最终资金'''
    if new:
        return resultdf.iloc[-1]['new_own cash']
    else:
        return resultdf.iloc[-1]['own cash']

def long_opr_rate(resultdf,new=False):
    '''多操作占比'''
    return resultdf.loc[resultdf['tradetype']==1].shape[0]/resultdf.shape[0]

def short_opr_rate(resultdf,new=False):
    '''空操作占比'''
    return resultdf.loc[resultdf['tradetype'] == -1].shape[0] / resultdf.shape[0]


def annual(resultdf,new=False):
    '''年化收益'''
    if new:
        cash_col='new_own cash'
        closeutc_col='new_closeutc'
        openutc_col='openutc'
    else:
        cash_col='own cash'
        closeutc_col='closeutc'
        openutc_col='openutc'
    return annual_return(resultdf, cash_col,closeutc_col,openutc_col)

def sharpe(resultdf,new=False):
    '''夏普比率'''
    if new:
        cash_col='new_own cash'
        closeutc_col='new_closeutc'
        retr_col='new_ret_r'
        openutc_col='openutc'
    else:
        cash_col='own cash'
        closeutc_col='closeutc'
        retr_col='ret_r'
        openutc_col='openutc'
    return sharpe_ratio(resultdf,cash_col,closeutc_col,retr_col,openutc_col)

def sr(resultdf,new=False):
    '''成功率'''
    if new:
        ret_col='new_ret'
    else:
        ret_col='ret'
    return success_rate(resultdf, ret_col)

def long_sr(resultdf,new=False):
    '''多操作成功率'''
    if new:
        ret_col='new_ret'
    else:
        ret_col='ret'
    df=resultdf.loc[resultdf['tradetype']==1]
    return success_rate(df,ret_col)

def short_sr(resultdf,new=False):
    '''空操作成功率'''
    if new:
        ret_col='new_ret'
    else:
        ret_col='ret'
    df=resultdf.loc[resultdf['tradetype']==-1]
    return success_rate(df,ret_col)

def draw_back(resultdf,new=False):
    '''最大回撤'''
    if new:
        cash_col='new_own cash'
        opentime_col='opentime'
    else:
        cash_col='own cash'
        opentime_col='opentime'
    return max_drawback(resultdf, cash_col, opentime_col)[0]

def max_single_earn_rate(resultdf,new=False):
    '''单次最大盈利'''
    if new:
        retr_col='new_ret_r'
    else:
        retr_col='ret_r'
    return resultdf[retr_col].max()

def max_single_loss_rate(resultdf,new=False):
    '''单次最大亏损'''
    if new:
        retr_col='new_ret_r'
    else:
        retr_col='ret_r'
    return resultdf[retr_col].min()

def profit_loss_rate(resultdf,new=False):
    '''盈亏比'''
    if new:
        ret_col = 'new_ret'
    else:
        ret_col = 'ret'
    avg_profit=resultdf.loc[resultdf[ret_col]>0,ret_col].mean()
    avg_loss = resultdf.loc[resultdf[ret_col]<0,ret_col].mean()
    return avg_profit/abs(avg_loss)

def long_profit_loss_rate(resultdf,new=False):
    '''多操作盈亏比'''
    if new:
        ret_col = 'new_ret'
    else:
        ret_col = 'ret'
    avg_profit=resultdf.loc[(resultdf[ret_col]>0)&(resultdf['tradetype']==1),ret_col].mean()
    avg_loss = resultdf.loc[(resultdf[ret_col]<0)&(resultdf['tradetype']==1),ret_col].mean()
    return avg_profit/abs(avg_loss)

def short_profit_loss_rate(resultdf,new=False):
    '''空操作盈亏比'''
    if new:
        ret_col = 'new_ret'
    else:
        ret_col = 'ret'
    avg_profit=resultdf.loc[(resultdf[ret_col]>0)&(resultdf['tradetype']==-1),ret_col].mean()
    avg_loss = resultdf.loc[(resultdf[ret_col]<0)&(resultdf['tradetype']==-1),ret_col].mean()
    return avg_profit/abs(avg_loss)

def successive_win(resultdf,new=False):
    '''连续盈利次数统计'''
    if new:
        ret_col='new_ret'
    else:
        ret_col='ret'
    df1 = pd.DataFrame()
    df1[ret_col] = resultdf[ret_col]
    df1['tradetype'] = resultdf['tradetype']
    df1['oprindex'] = np.arange(df1.shape[0])
    df1['win'] = -1
    df1.loc[df1[ret_col] > 0, 'win'] = 1
    df1['win_shift1'] = df1['win'].shift(1).fillna(0)
    df1['win_cross'] = 0
    df1.loc[df1['win'] != df1['win_shift1'], 'win_cross'] = df1['oprindex']
    df1.ix[0, 'win_cross'] = 1
    df2 = pd.DataFrame()
    df2['oprindex'] = df1.loc[df1['win_cross'] != 0, 'oprindex']
    df2[ret_col] = df1.loc[df1['win_cross'] != 0,ret_col]
    df2['count'] = df2['oprindex'].shift(-1).fillna(0) - df2['oprindex']
    df2.ix[df2.iloc[-1].oprindex, 'count'] = 0
    win_count = df2.loc[df2[ret_col] > 0, 'count']
    loss_count = df2.loc[df2[ret_col] <= 0, 'count']
    return {
        "MaxSuccessiveEarn":win_count.max(),
        "MaxSuccessiveLoss":loss_count.max(),
        "AvgSuccessiveEarn":win_count.mean(),
        "AveSuccessiveLoss":loss_count.mean()
    }

ResultIndexFucnMap={
    "OprTimes": opr_times,  # 操作次数
    "LongOprTimes": long_opr_times,  # 多操作次数
    "ShortOprTimes": short_opr_times,  # 空操作次数
    "EndCash": end_cash,  # 最终资金
    "LongOprRate": long_opr_rate,  # 多操作占比
    "ShortOprRate": short_opr_rate,  # 空操作占比
    "Annual": annual,  # 年化收益
    "Sharpe": sharpe,  # 夏普
    "SR": sr,  # 成功率
    "LongSR": long_sr,  # 多操作成功率
    "ShortSR": short_sr,  # 空操作成功率
    "DrawBack": draw_back,  # 资金最大回撤
    "MaxSingleEarnRate": max_single_earn_rate,  # 单次最大盈利率
    "MaxSingleLossRate": max_single_loss_rate,  # 单次最大亏损率
    "ProfitLossRate": profit_loss_rate,  # 盈亏比
    "LongProfitLossRate": long_profit_loss_rate,  # 多操作盈亏比
    "ShoartProfitLossRate": short_profit_loss_rate,  # 空操作盈亏比
}

def getStatisticsResult(resultdf,new,indexlist):
    '''计算统计结果
    连续盈亏次数的计算量比较大并且重复，所以一次算好4个备用
    下面4个指标单独计算，其他的使用map的函数直接返回结果
    "MaxSuccessiveEarn": True,  # 最大连续盈利次数
    "MaxSuccessiveLoss": True,  # 最大连续亏损次数
    "AvgSuccessiveEarn": True,  # 平均连续盈利次数
    "AveSuccessiveLoss": True  # 平均连续亏损次数'
    '''
    r=[]
    successive_result={}#用来保存连续盈亏次数计算结果
    for d in indexlist:
        if d in ["MaxSuccessiveEarn","MaxSuccessiveLoss","AvgSuccessiveEarn","AveSuccessiveLoss"]:
            if not successive_result:
                successive_result=successive_win(resultdf,new)
            r.append(successive_result[d])
        else:
            func=ResultIndexFucnMap[d]
            r.append(func(resultdf,new))
    return r

def calcResult(result,symbolinfo,initialCash,positionRatio,ret_col='ret'):
    '''计算交易结果'''

    multiplier = symbolinfo.getMultiplier() #乘数
    poundgeType, poundgeFee, poundgeRate = symbolinfo.getPoundage() #手续费率
    marginRatio=symbolinfo.getMarginRatio() #保证金率

    newresult=pd.DataFrame()
    newresult['ret']=result[ret_col]
    newresult['openprice']=result['openprice']
    newresult['commission_fee'] = 0 #手续费
    newresult['per earn'] = 0  # 单笔盈亏
    newresult['own cash'] = 0  # 自有资金线
    newresult['hands'] = 0 #每次手数

    #计算第一次交易的结果
    availableFund = initialCash*positionRatio
    cashPerHand = newresult.ix[0,'openprice'] * multiplier
    hands=availableFund//(cashPerHand*marginRatio)
    if poundgeType == symbolinfo.POUNDGE_TYPE_RATE:
        newresult.ix[0, 'commission_fee'] = cashPerHand * hands * poundgeRate * 2
    else:
        newresult.ix[0, 'commission_fee'] = hands * poundgeFee * 2
    newresult.ix[0, 'per earn'] = newresult.ix[0, 'ret'] * hands * multiplier
    newresult.ix[0, 'own cash'] = initialCash + newresult.ix[0, 'per earn'] - newresult.ix[0, 'commission_fee']
    newresult.ix[0, 'hands'] = hands

    #计算后续交易的结果
    oprtimes = newresult.shape[0]
    for i in range(1, oprtimes):
        lastOwnCash=newresult.ix[i-1,'own cash']
        availableFund = lastOwnCash * positionRatio #本次可用资金等于上一次操作后的资金*持仓率
        cashPerHand = newresult.ix[i, 'openprice'] * multiplier
        hands = availableFund // (cashPerHand * marginRatio)
        if poundgeType == symbolinfo.POUNDGE_TYPE_RATE:
            commission = cashPerHand * hands * poundgeRate * 2
        else:
            commission = hands * poundgeFee * 2
        newresult.ix[i,'commission_fee'] = commission
        newresult.ix[i, 'per earn'] = newresult.ix[i, 'ret'] * hands * multiplier
        newresult.ix[i, 'own cash'] = lastOwnCash + newresult.ix[i, 'per earn'] - commission
        newresult.ix[i, 'hands'] = hands

    return newresult['commission_fee'],newresult['per earn'],newresult['own cash'],newresult['hands']

if __name__ == '__main__':
    resultdf=pd.read_csv('D:\\002 MakeLive\myquant\LvyiWin\Results\SHFE RB600 slip\SHFE.RB600 Set6213 MS4 ML21 KN24 DN30 result.csv')
    #print annual_return(resultdf)
    #max_drawback(resultdf)
    #average_change(resultdf)
    #print('max_up:%d,max_down:%d'%(max_successive_up(resultdf)))
    #print('max_return:%.2f,min_return:%.2f'%(max_period_return(resultdf)))
    #print volatility(resultdf)
    print sharpe_ratio(resultdf)
    #print success_rate(resultdf)