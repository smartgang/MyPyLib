# -*- coding: utf-8 -*-
'''
每次操作设置固定的止损比例，亏损达到该比例即平仓止损

代码流程：
遍历每一次操作
每次操作内，计算截止当前的盈利
取亏损达到目标值的第一个值，即为固定比例止损价
把止损价round到整数价
'''
import pandas as pd
import ResultStatistics as RS
import DATA_CONSTANTS as DC

def getLongFixRateLossByTick(bardf,openprice,fixRate):
    '''
    1.计算截至当前盈利
    2.取亏损达到目标值的第一个值
    '''
    df = pd.DataFrame({'high': bardf['longHigh'],'low':bardf['longLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['lossRate']=df['low']/openprice-1
    df2=df.loc[df['lossRate']<=fixRate]
    if df2.shape[0]>0:
        temp=df2.iloc[0]
        newcloseprice = temp['low']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
        return newcloseprice,strtime,utctime,timeindex
    return 0,' ',0,0


def getShortFixRateLossByTick(bardf,openprice,fixRate):
    '''
    1.计算截至当前盈利
    2.取亏损达到目标值的第一个值
    '''
    df = pd.DataFrame({'high': bardf['shortHigh'],'low':bardf['shortLow'], 'strtime': bardf['strtime'], 'utc_time': bardf['utc_time'],
                       'timeindex': bardf['Unnamed: 0']})
    df['lossRate']=1-df['high']/openprice
    df2=df.loc[df['lossRate']<=fixRate]
    if df2.shape[0]>0:
        temp=df2.iloc[0]
        newcloseprice = temp['high']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
        return newcloseprice,strtime,utctime,timeindex
    return 0,' ',0,0

#==========================================================================================================
def getLongFixRateLossByRealtick(tickdf,openprice,fixRate):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time'],
                       'timeindex': tickdf['Unnamed: 0']})
    df['lossRate']=df['close']/openprice-1
    df2=df.loc[df['lossRate']<=fixRate]
    if df2.shape[0]>0:
        temp=df2.iloc[0]
        newcloseprice = temp['close']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
        return newcloseprice,strtime,utctime,timeindex
    return 0,' ',0,0


def getShortFixRateLossByRealtick(tickdf,openprice,fixRate):
    df = pd.DataFrame({'close': tickdf.last_price, 'strtime': tickdf['strtime'], 'utc_time': tickdf['utc_time'],
                       'timeindex': tickdf['Unnamed: 0']})
    df['lossRate']=1-df['close']/openprice
    df2=df.loc[df['lossRate']<=fixRate]
    if df2.shape[0]>0:
        temp=df2.iloc[0]
        newcloseprice = temp['close']
        strtime = temp['strtime']
        utctime = temp['utc_time']
        timeindex = temp['timeindex']
        return newcloseprice,strtime,utctime,timeindex
    return 0,' ',0,0

def frslCalRealTick(strategyName,symbol,K_MIN,setname,ticksupplier,barxm,fixRate,slip,tofolder):
    print 'ownl;', str(fixRate), ',setname:', setname
    oprdf = pd.read_csv(strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    tickstartutc,tickendutc=ticksupplier.getDateUtcRange()
    #只截取tick时间范围内的opr
    oprdf = oprdf.loc[(oprdf['openutc'] > tickstartutc) & (oprdf['openutc'] < tickendutc)]

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    #oprnum = oprdf.shape[0]
    oprindex = oprdf.index.tolist()
    worknum=0
    #for i in range(oprnum):
    for i in oprindex:
        opr = oprdf.loc[i]
        startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime #从开仓的10m线结束后开始
        endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime#一直到平仓的10m线结束
        oprtype = opr.tradetype
        openprice = opr.openprice
        tickdata = ticksupplier.getTickDataByUtc(startutc, endutc)
        if oprtype == 1:
            newcloseprice, strtime, utctime, timeindex = getLongFixRateLossByRealtick(tickdata,openprice,fixRate)
            if newcloseprice !=0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

        else:
            newcloseprice, strtime, utctime, timeindex = getShortFixRateLossByRealtick(tickdata, openprice, fixRate)
            if newcloseprice != 0:
                oprdf.ix[i, 'new_closeprice'] = newcloseprice
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['retdelta'] = oprdf['new_ret'] - oprdf['ret']
    oprdf.to_csv(tofolder + strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' resultFRSL_by_realtick.csv')

#================================================================================================
def frslCal(strategyName,symbolInfo,K_MIN,setname,bar1m,barxm,fixRate,positionRatio,initialCash,tofolder,indexcols):
    print 'frsl;', str(fixRate), ',setname:', setname
    symbol=symbolInfo.symbol
    pricetick=symbolInfo.getPriceTick()
    oprdf = pd.read_csv(strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
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
            newcloseprice, strtime, utctime, timeindex = getLongFixRateLossByTick(data1m,openprice,fixRate)
            if newcloseprice !=0:
                ticknum=(openprice*fixRate//pricetick)-1
                oprdf.ix[i, 'new_closeprice'] = openprice + ticknum*pricetick
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

        else:
            newcloseprice, strtime, utctime, timeindex = getShortFixRateLossByTick(data1m, openprice, fixRate)
            if newcloseprice != 0:
                ticknum=(openprice*fixRate//pricetick)-1
                oprdf.ix[i, 'new_closeprice'] = openprice - ticknum*pricetick
                oprdf.ix[i, 'new_closetime'] = strtime
                oprdf.ix[i, 'new_closeindex'] = timeindex
                oprdf.ix[i, 'new_closeutc'] = utctime
                worknum+=1

    slip=symbolInfo.getSlip()
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                      symbolInfo,
                                                                                                      initialCash,
                                                                                                      positionRatio,ret_col='new_ret')
    #保存新的result文档
    oprdf.to_csv(tofolder+strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' resultFRSL_by_tick.csv')

    olddailydf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv',index_col='date')
    #计算统计结果
    oldr = RS.getStatisticsResult(oprdf, False, indexcols,olddailydf)

    dailyK=DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder+strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresultFRSL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf,True,indexcols,dR.dailyClose)
    del oprdf
    return [setname,fixRate,worknum]+oldr+newr

#================================================================================================
def progressFrslCal(strategyName,symbolInfo,K_MIN,setname,bar1m,barxm,fixRate,positionRatio,initialCash,tofolder,indexcols):
    '''
    增量式止损
    '''
    print 'frsl;', str(fixRate), ',setname:', setname
    symbol=symbolInfo.symbol
    pricetick=symbolInfo.getPriceTick()
    orioprdf = pd.read_csv(strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    orioprnum = orioprdf.shape[0]
    frsldf=pd.read_csv(tofolder + strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' resultFRSL_by_tick.csv')
    frsldf.drop('Unnamed: 0.1',axis=1,inplace=True)
    frsloprnum=frsldf.shape[0]
    oprdf=frsldf
    if orioprnum>frsloprnum:
        oprdf=orioprdf.loc[frsloprnum:,:]
        oprdf['new_closeprice'] = oprdf['closeprice']
        oprdf['new_closetime'] = oprdf['closetime']
        oprdf['new_closeindex'] = oprdf['closeindex']
        oprdf['new_closeutc'] = oprdf['closeutc']
        oprdf['max_opr_gain'] = 0 #本次操作期间的最大收益
        oprdf['min_opr_gain'] = 0#本次操作期间的最小收益
        oprdf['max_dd'] = 0
        for i in range(frsloprnum,orioprnum):
            opr = oprdf.loc[i]
            startutc = (barxm.loc[barxm['utc_time'] == opr.openutc]).iloc[0].utc_endtime - 60#从开仓的10m线结束后开始
            endutc = (barxm.loc[barxm['utc_time'] == opr.closeutc]).iloc[0].utc_endtime#一直到平仓的10m线结束
            oprtype = opr.tradetype
            openprice = opr.openprice
            data1m = bar1m.loc[(bar1m['utc_time'] > startutc) & (bar1m['utc_time'] < endutc)]
            if oprtype == 1:
                newcloseprice, strtime, utctime, timeindex = getLongFixRateLossByTick(data1m,openprice,fixRate)
                if newcloseprice !=0:
                    ticknum = (openprice * fixRate // pricetick) - 1
                    oprdf.ix[i, 'new_closeprice'] = openprice + ticknum * pricetick
                    oprdf.ix[i, 'new_closetime'] = strtime
                    oprdf.ix[i, 'new_closeindex'] = timeindex
                    oprdf.ix[i, 'new_closeutc'] = utctime
            else:
                newcloseprice, strtime, utctime, timeindex = getShortFixRateLossByTick(data1m, openprice, fixRate)
                if newcloseprice != 0:
                    ticknum = (openprice * fixRate // pricetick) - 1
                    oprdf.ix[i, 'new_closeprice'] = openprice - ticknum * pricetick
                    oprdf.ix[i, 'new_closetime'] = strtime
                    oprdf.ix[i, 'new_closeindex'] = timeindex
                    oprdf.ix[i, 'new_closeutc'] = utctime

        slip=symbolInfo.getSlip()
        # 2017-12-08:加入滑点
        oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
        oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
        oprdf['new_commission_fee']=0
        oprdf['new_per earn']=0
        oprdf['new_own cash']=0
        oprdf['new_hands'] = 0
        oprdf=pd.concat([frsldf,oprdf])
        oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                          symbolInfo,
                                                                                                          initialCash,
                                                                                                          positionRatio,ret_col='new_ret')
        #保存新的result文档
        oprdf.to_csv(tofolder+strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' resultFRSL_by_tick.csv')

    #计算统计结果
    worknum = oprdf.loc[oprdf['new_closeindex']!=oprdf['closeindex']].shape[0]
    olddailydf = pd.read_csv(strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv',index_col='date')
    oldr = RS.getStatisticsResult(oprdf, False, indexcols,olddailydf)

    dailyK=DC.generatDailyClose(barxm)
    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv((tofolder+strategyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresultFRSL_by_tick.csv'))
    newr = RS.getStatisticsResult(oprdf,True,indexcols,dR.dailyClose)
    del oprdf
    del orioprdf
    del frsldf
    return [setname, fixRate, worknum] + oldr + newr
if __name__ == '__main__':
    pass