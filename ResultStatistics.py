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

def annual_return(resultdf):
    '''
    计算年化收益
    :param resultdf:包含所有操作结果
    公式：（账户最终价值/账户初始价值）^（250/回测期间总天数）-1
    :return:annual_return
    '''
    oprnum=resultdf.shape[0]
    #startcash=resultdf.ix[0,'own cash']
    startcash=20000
    startdate=date.fromtimestamp(resultdf.ix[0,'openutc'])
    endcash=resultdf.ix[oprnum-1,'own cash']
    enddate=date.fromtimestamp(resultdf.ix[oprnum-1,'closeutc'])
    datenum=float((enddate-startdate).days)
    return pow(endcash/startcash,250/datenum)-1

def max_drawback(resultdf):
    '''
    最大回撤
    :param resultdf:
    公式：最大回撤就是从一个高点到一个低点最大的下跌幅度
    :return:
    '''
    df=pd.DataFrame({'date':resultdf.opentime,'capital':resultdf['own cash']})

    #df['max2here']=pd.expanding_max(df['capital'])
    df['max2here']=df['capital'].expanding().max()
    df['dd2here']=df['capital']/df['max2here']-1

    temp= df.sort_values(by='dd2here').iloc[0][['date','dd2here']]
    max_dd=temp['dd2here']
    end_date=temp['date']

    df=df[df['date']<=end_date]
    start_date=df.sort_values(by='capital',ascending=False).iloc[0]['date']
    print('最大回撤为：%f,开始时间：%s,结果时间:%s'% (max_dd,start_date,end_date))

def average_change(resultdf):
    '''
    平均涨幅
    :param resultdf:
    :return:账户收益的平均值
    '''
    average=resultdf['ret_r'].mean()
    return average

def max_successive_up(resultdf):
    '''
    最大连续上涨和下跌的次数
    :param resultdf:
    :return:
    '''
    ret = resultdf.ret.tolist()
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

def max_period_return(resultdf):
    '''
    单次最大收益率和最大亏损率
    :param resultdf:
    :return:
    '''
    max_return=resultdf.ret_r.max()
    min_return=resultdf.ret_r.min()
    return max_return,min_return

def volatility(resultdf):
    '''
    计算收益波动率:账户日收益的年化标准差
    :param resultdf:
    :return:
    '''
    from math import sqrt
    vol = resultdf.ret_r.std() * sqrt(250)
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

def sharpe_ratio(resultdf):
    '''
    计算夏普比率:（账户年化收益率-无风险利率）/ 收益波动率。
    :param resultdf:
    :return:
    '''
    #10年期国债年化收益率
    rf=0.0284

    #计算年化收益
    oprnum=resultdf.shape[0]
    #startcash=resultdf.ix[0,'own cash']
    startcash=20000
    startdate=date.fromtimestamp(resultdf.ix[0,'openutc'])
    endcash=resultdf.ix[oprnum-1,'own cash']
    enddate=date.fromtimestamp(resultdf.ix[oprnum-1,'closeutc'])
    datenum=float((enddate-startdate).days)
    annual_return = pow(endcash/startcash,250/datenum)-1

    #计算波动率
    from math import sqrt
    vol = resultdf.ret_r.std() * sqrt(250)

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

def success_rate(resultdf):
    '''
    计算所有操作的成功率，ret>0为成功
    :param resultdf:
    :return:
    '''
    successcount=resultdf.loc[resultdf['ret']>0].shape[0]
    totalcount=resultdf.shape[0]
    return successcount/float(totalcount)

if __name__ == '__main__':
    resultdf=pd.read_csv('D:\\002 MakeLive\myquant\LvyiWin\Results\SHFE RB600 slip\SHFE.RB600 Set6213 MS4 ML21 KN24 DN30 result.csv')
    #print annual_return(resultdf)
    #max_drawback(resultdf)
    #average_change(resultdf)
    #print('max_up:%d,max_down:%d'%(max_successive_up(resultdf)))
    #print('max_return:%.2f,min_return:%.2f'%(max_period_return(resultdf)))
    #print volatility(resultdf)
    #print sharpe_ratio(resultdf)
    print success_rate(resultdf)