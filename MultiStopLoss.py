# -*- coding: utf-8 -*-
'''
多目标综合止损
'''
import pandas as pd
import DATA_CONSTANTS as DC
import numpy as np
import os
import ResultStatistics as RS
import multiprocessing
from DynamicStopLoss import *
from OnceWinNoLoss import  *

def multiStopLosslCal(stratetyName,symbolInfo,K_MIN,setname,stopLossTargetDictList,barxmdic, result_para_dic,tofolder,indexcols):
    print 'setname:', setname
    symbol=symbolInfo.domain_symbol
    oprdf = pd.read_csv(stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result.csv')

    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(oprdf)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(),barxmdic, symbolDomainDic)
    dailyK = DC.generatDailyClose(barxm)

    positionRatio = result_para_dic['positionRatio']
    initialCash = result_para_dic['initialCash']

    oprlist=[]
    sltnum=len(stopLossTargetDictList)
    for i in range(sltnum):
        slt=stopLossTargetDictList[i]
        #遍历读取各止损目标的结果文件,按名称将结果写入oprdf中
        sltdf=pd.read_csv("%s%s %s%d %s %s"%(slt['folder'],stratetyName,symbol,K_MIN,setname,slt['fileSuffix']))
        sltName=slt['name']
        oprdf[sltName+'_closeprice'] = sltdf['new_closeprice']
        oprdf[sltName+'_closetime'] = sltdf['new_closetime']
        oprdf[sltName+'_closeindex'] = sltdf['new_closeindex']
        oprdf[sltName+'_closeutc'] = sltdf['new_closeutc']
        oprdf[sltName+'_ret'] = sltdf['new_ret']
        oprdf[sltName+'_own cash'] = sltdf['new_own cash']
        oprlist.append(sltdf)
    #dsloprname=stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' resultDSL_by_tick.csv'
    #ownloprname=stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' resultOWNL_by_tick.csv'
    #dsloprdf=pd.read_csv(dslFolder+dsloprname)
    #ownloprdf=pd.read_csv(ownlFolder+ownloprname)

    oprdf['new_closeprice'] = oprdf['closeprice']
    oprdf['new_closetime'] = oprdf['closetime']
    oprdf['new_closeindex'] = oprdf['closeindex']
    oprdf['new_closeutc'] = oprdf['closeutc']
    oprdf['min_closeutc'] = oprdf['closeutc']
    oprdf['max_closeutc'] = oprdf['closeutc']
    for i in range(sltnum):
        #先取最早平仓的时间，再根据时间去匹配类型
        slt=stopLossTargetDictList[i]
        utcname=slt['name']+'_closeutc'
        oprdf['min_closeutc']=oprdf.loc[:,['min_closeutc',utcname]].min(axis=1)
        oprdf['max_closeutc']=oprdf.loc[:,['max_closeutc',utcname]].max(axis=1)
    #根据最早平仓时间的结果，匹配平仓类型,不处理时间相同的情况
    oprdf['closetype']='Normal'
    oprdf.loc[oprdf['max_closeutc']!=oprdf['closeutc'],'min_closeutc'] = oprdf['max_closeutc']
    for i in range(sltnum):
        slt=stopLossTargetDictList[i]
        name=slt['name']
        utcname= name + '_closeutc'
        utcnamebuf = name + '_closeutc_buf'
        oprdf[utcnamebuf]= oprdf[utcname]
        oprdf.loc[(oprdf['max_closeutc']!=oprdf['closeutc']) & (oprdf[utcname]==oprdf['closeutc']),utcnamebuf]=oprdf['max_closeutc']
    for i in range(sltnum):
        #先取最早平仓的时间，再根据时间去匹配类型
        slt=stopLossTargetDictList[i]
        name = slt['name']
        utcnamebuf = name + '_closeutc_buf'
        oprdf['min_closeutc']=oprdf.loc[:,['min_closeutc',utcnamebuf]].min(axis=1)
    for i in range(sltnum):
        #先按与最小相同的标识名称，因为止损文件中没有生效的操作的值与原值相同
        #所以标识完后剩下的Normal就是原时间比止损时间早的值（也就是使用最小值匹配不出来的值，需要特殊处理）
        slt=stopLossTargetDictList[i]
        name=slt['name']
        utcname=name+'_closeutc'
        oprdf.loc[oprdf['min_closeutc']==oprdf[utcname],'closetype'] = slt['name']
        oprdf.loc[oprdf['min_closeutc']==oprdf[utcname], 'new_closeprice']= oprdf[name+'_closeprice']
        oprdf.loc[oprdf['min_closeutc']==oprdf[utcname],'new_closetime'] = oprdf[name+'_closetime']
        oprdf.loc[oprdf['min_closeutc']==oprdf[utcname],'new_closeindex'] = oprdf[name+'_closeindex']
        oprdf.loc[oprdf['min_closeutc']==oprdf[utcname],'new_closeutc'] = oprdf[name+'_closeutc']

        oprdf.drop(name+'_closeutc_buf',axis=1,inplace=True)#删掉buf列
    #标识正常止损
    oprdf.loc[oprdf['min_closeutc'] == oprdf['closeutc'], 'closetype'] = 'Normal'
    oprdf.drop('min_closeutc',axis=1,inplace=True)
    oprdf.drop('max_closeutc',axis=1,inplace=True)
    slip=symbolInfo.getSlip()
    # 2017-12-08:加入滑点
    oprdf['new_ret'] = ((oprdf['new_closeprice'] - oprdf['openprice']) * oprdf['tradetype']) - slip
    oprdf['new_ret_r'] = oprdf['new_ret'] / oprdf['openprice']
    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                      symbolInfo,
                                                                                                      initialCash,
                                                                                                      positionRatio,ret_col='new_ret')
    oprdf.to_csv(tofolder+'\\'+stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result_multiSLT.csv', index=False)

    #计算统计结果
    slWorkNum=oprdf.loc[oprdf['closetype']!='Normal'].shape[0]
    olddailydf = pd.read_csv(stratetyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv',index_col='date')
    oldr = RS.getStatisticsResult(oprdf, False, indexcols,olddailydf)

    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv(tofolder+'\\'+stratetyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult_multiSLT.csv')
    newr = RS.getStatisticsResult(oprdf,True,indexcols,dR.dailyClose)

    return [setname,tofolder,slWorkNum,] + oldr + newr


def multiStopLosslCal_remove_polar(stratetyName,symbolInfo,K_MIN,setname,stopLossTargetDictList,barxmdic, positionRatio,initialCash,tofolder,indexcols):
    print 'setname:', setname
    symbol=symbolInfo.domain_symbol
    oprdf = pd.read_csv(stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    oprdf = RS.opr_result_remove_polar(oprdf=oprdf, new_cols=True)
    symbolDomainDic = symbolInfo.amendSymbolDomainDicByOpr(oprdf)
    barxm = DC.getDomainbarByDomainSymbol(symbolInfo.getSymbolList(),barxmdic, symbolDomainDic)
    dailyK = DC.generatDailyClose(barxm)

    oprdf['new_commission_fee'], oprdf['new_per earn'], oprdf['new_own cash'], oprdf['new_hands'] = RS.calcResult(oprdf,
                                                                                                      symbolInfo,
                                                                                                      initialCash,
                                                                                                      positionRatio,ret_col='new_ret')
    oprdf.to_csv(tofolder+'\\'+stratetyName+' '+symbol + str(K_MIN) + ' ' + setname + ' result_multiSLT_remove_polar.csv', index=False)

    #计算统计结果
    slWorkNum=oprdf.loc[oprdf['closetype']!='Normal'].shape[0]
    olddailydf = pd.read_csv(stratetyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult.csv',index_col='date')
    oldr = RS.getStatisticsResult(oprdf, False, indexcols,olddailydf)

    dR = RS.dailyReturn(symbolInfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
    dR.calDailyResult()
    dR.dailyClose.to_csv(tofolder+'\\'+stratetyName + ' ' + symbol + str(K_MIN) + ' ' + setname + ' dailyresult_multiSLT_remove_polar.csv')
    newr = RS.getStatisticsResult(oprdf,True,indexcols,dR.dailyClose)

    return [setname,tofolder,slWorkNum,] + oldr + newr

if __name__ == '__main__':
    pass