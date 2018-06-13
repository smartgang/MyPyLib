# -*- coding: utf-8 -*-
'''
多目标推进：
    通过utctime来截取数据：通过月份生成utc
    每读一个set文件把全部推进都做完，以减少文件读取次数
    每一次推进，将所有目标都计算完，保存到list中，每个set做完之后，每个目标的list根据推进月份有一行,append到总list中
    推进完后将总list使用df，每个目标一个df
    再在各个df中按列算排名和总分，保存到一个总分的df中
'''
import pandas as pd
import numpy as np
import time
import ResultStatistics as RS
from datetime import datetime
import multiprocessing
import DATA_CONSTANTS as DC
import os

def calWhiteResult(strategyName,whiteWindows,symbolinfo,K_MIN,parasetlist,monthlist,datapath,resultpath,columns,filesuffix='result.csv'):
    '''
    根据月列表和白色窗口大小，计算推进过程中，每一期白区4个目标分别的结果，保存到4个result文件中
    :return:
    '''
    print ('WhiteWindows:%d calculating forward result.'% whiteWindows)
    symbol=symbolinfo.domain_symbol
    parasetlen = parasetlist.shape[0]
    annual_total_list=[]
    sharpe_total_list=[]
    success_rate_total_list=[]
    drawback_total_list=[]
    cash_col =columns['cash_col']
    closeutc_col = columns['closeutc_col']
    retr_col = columns['retr_col']
    ret_col = columns['ret_col']

    for i in np.arange(0, parasetlen):
        setname=parasetlist.ix[i,'Setname']
        #print setname
        filename=datapath+strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' '+filesuffix
        resultdf=pd.read_csv(filename)

        dailydf=pd.read_csv(datapath+strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' daily'+filesuffix)

        annual_list=[]
        sharpe_list=[]
        success_rate_list=[]
        drawback_list=[]
        annual_list.append(setname)
        sharpe_list.append(setname)
        success_rate_list.append(setname)
        drawback_list.append(setname)
        for i in range(12,len(monthlist)-1):#修改为从第13个月开始
            whiteWindowsStart=monthlist[i-whiteWindows]+'-01 00:00:00'
            whiteWindowsEnd=monthlist[i]+'-01 00:00:00'#从取end月份的1日，表示的是i-1个月的数据
            startutc=float(time.mktime(time.strptime(whiteWindowsStart,"%Y-%m-%d %H:%M:%S")))
            endutc=float(time.mktime(time.strptime(whiteWindowsEnd,"%Y-%m-%d %H:%M:%S")))
            resultdata=resultdf.loc[(resultdf['openutc']>=startutc) & (resultdf['openutc']<endutc)]
            dailydata = dailydf.loc[(dailydf['utc_time']>=startutc) & (dailydf['utc_time']<endutc)]
            if resultdata.shape[0]>0:
                resultdata=resultdata.reset_index(drop=True)
                #annual_list.append(RS.annual_return(resultdata,cash_col=cash_col,closeutc_col=closeutc_col))
                #sharpe_list.append(RS.sharpe_ratio(resultdata,cash_col=cash_col,closeutc_col=closeutc_col,retr_col=retr_col))
                annual_list.append(RS.annual(dailydata))
                sharpe_list.append(RS.sharpe(dailydata))
                success_rate_list.append(RS.success_rate(resultdata,ret_col=ret_col))
                drawback,a,b=RS.max_drawback(resultdata,cash_col=cash_col)
                drawback_list.append(drawback)
            else:
                annual_list.append(0)
                sharpe_list.append(0)
                success_rate_list.append(0)
                drawback_list.append(-1)
            #print('annual:%f.2,sharpe:%f.2,success_rate:%f.2,drawback:%f.2'%(annual,sharpe,success_rate,drawback))
        annual_total_list.append(annual_list)
        sharpe_total_list.append(sharpe_list)
        success_rate_total_list.append(success_rate_list)
        drawback_total_list.append(drawback_list)

    colname=[]
    colname.append('Setname')
    for m in monthlist[12:-1]:
        colname.append(m)
    annualdf=pd.DataFrame(annual_total_list,columns=colname)
    sharpedf=pd.DataFrame(sharpe_total_list,columns=colname)
    successdf=pd.DataFrame(success_rate_total_list,columns=colname)
    drawbackdf=pd.DataFrame(drawback_total_list,columns=colname)
    filenamehead=("%s_%s_%s_%d_win%d_"%(resultpath,strategyName,symbol,K_MIN,whiteWindows))
    annualdf.to_csv(filenamehead+'annual_result.csv')
    sharpedf.to_csv(filenamehead+'sharpe_result.csv')
    successdf.to_csv(filenamehead+'success_rate_result.csv')
    drawbackdf.to_csv(filenamehead+'drawback_result.csv')

def rankByWhiteResult(strategyName,symbolinfo,K_MIN,whiteWindows,datapath,resultpath):
    '''
    对目标结果进行排序打分
    按列来，每列排序重置顺序，再加分
    :return:
    '''
    print ('WhiteWindows:%d calculating rank result!'% whiteWindows)
    symbol=symbolinfo.domain_symbol
    datanamehead = ("%s_%s_%s_%d_win%d_" % (datapath, strategyName,symbol, K_MIN, whiteWindows))
    annualdf=pd.read_csv(datanamehead+'annual_result.csv')
    sharpedf=pd.read_csv(datanamehead+'sharpe_result.csv')
    successdf=pd.read_csv(datanamehead+'success_rate_result.csv')
    drawbackdf=pd.read_csv(datanamehead+'drawback_result.csv')
    col = annualdf.columns.tolist()[2:]
    annualrankdf=pd.DataFrame({'Setname':annualdf.Setname})
    sharperankdf = pd.DataFrame({'Setname': annualdf.Setname})
    successrankdf = pd.DataFrame({'Setname':annualdf.Setname})
    drawbackrankdf = pd.DataFrame({'Setname':annualdf.Setname})
    rank1df = pd.DataFrame({'Setname':annualdf.Setname})
    rank2df = pd.DataFrame({'Setname': annualdf.Setname})
    rank3df = pd.DataFrame({'Setname': annualdf.Setname})
    rank4df = pd.DataFrame({'Setname': annualdf.Setname})
    rank5df = pd.DataFrame({'Setname': annualdf.Setname})
    rank6df = pd.DataFrame({'Setname': annualdf.Setname})
    rank7df = pd.DataFrame({'Setname': annualdf.Setname})
    annualrankdf = annualrankdf.sort_values(by='Setname', ascending=True)
    sharperankdf = sharperankdf.sort_values(by='Setname', ascending=True)
    successrankdf = successrankdf.sort_values(by='Setname', ascending=True)
    drawbackrankdf = drawbackrankdf.sort_values(by='Setname', ascending=True)
    rank1df = rank1df.sort_values(by='Setname', ascending=True)
    rank2df = rank2df.sort_values(by='Setname', ascending=True)
    rank3df = rank3df.sort_values(by='Setname', ascending=True)
    rank4df = rank4df.sort_values(by='Setname', ascending=True)
    rank5df = rank5df.sort_values(by='Setname', ascending=True)
    rank6df = rank6df.sort_values(by='Setname', ascending=True)
    rank7df = rank7df.sort_values(by='Setname', ascending=True)
    for month in col:
        df = pd.DataFrame({'Setname': annualdf.Setname, 'annual': annualdf[month], 'sharpe': sharpedf[month],'success_rate':successdf[month],'drawback':drawbackdf[month]})
        df['AnnualRank'] = 0
        df['SharpeRank']=0
        df['SuccessRank']=0
        df['DrawbackRank']=0
        df['Rank1']=0#5个目标集的评分
        df['Rank2']=0
        df['Rank3']=0
        df['Rank4']=0
        df['Rank5']=0
        df['Rank6']=0
        df['Rank7']=0

        rangarray = range(df.shape[0], 0, -1)
        df = df.sort_values(by='annual', ascending=False)
        df['AnnualRank']+=rangarray
        df = df.sort_values(by='sharpe', ascending=False)
        df['SharpeRank'] += rangarray
        df = df.sort_values(by='success_rate', ascending=False)
        df['SuccessRank'] += rangarray
        df = df.sort_values(by='drawback', ascending=False)
        df['DrawbackRank'] += rangarray
        df = df.sort_values(by='Setname', ascending=True)
        df['Rank1']=df['AnnualRank']#目标集1：年化收益
        df['Rank2']=df['SharpeRank']#目标集2：夏普值
        df['Rank3']=df['AnnualRank']*0.6+df['DrawbackRank']*0.4#目标集3：年化收益*0.6+最大回撤*0.4
        df['Rank4']=df['SharpeRank']*0.6+df['DrawbackRank']*0.4#目标集4：夏普*0.6+最大回撤*0.4
        df['Rank5']=df['AnnualRank']*0.4+df['SharpeRank']*0.3+df['SuccessRank']*0.1+df['DrawbackRank']*0.2#目标集5：4目标综合
        df['Rank6']=df['SuccessRank']
        df['Rank7']=df['SuccessRank']*0.5+df['AnnualRank']*0.5

        annualrankdf[month]=df['AnnualRank']
        sharperankdf[month]=df['SharpeRank']
        successrankdf[month]=df['SuccessRank']
        drawbackrankdf[month]=df['DrawbackRank']
        rank1df[month]=df['Rank1']
        rank2df[month] = df['Rank2']
        rank3df[month] = df['Rank3']
        rank4df[month] = df['Rank4']
        rank5df[month] = df['Rank5']
        rank6df[month] = df['Rank6']
        rank7df[month] = df['Rank7']
        del df
    resultnamehead = ("%s_%s_%s_%d_win%d_" % (resultpath, strategyName,symbol, K_MIN, whiteWindows))
    annualrankdf.to_csv(resultnamehead+'AnnualRank.csv')
    sharperankdf.to_csv(resultnamehead+'SharpeRank.csv')
    successrankdf.to_csv(resultnamehead+'SuccessRank.csv')
    drawbackrankdf.to_csv(resultnamehead+'DrawbackRank.csv')
    rank1df.to_csv(resultnamehead+'Rank1.csv')
    rank2df.to_csv(resultnamehead+'Rank2.csv')
    rank3df.to_csv(resultnamehead+'Rank3.csv')
    rank4df.to_csv(resultnamehead+'Rank4.csv')
    rank5df.to_csv(resultnamehead+'Rank5.csv')
    rank6df.to_csv(resultnamehead + 'Rank6.csv')
    rank7df.to_csv(resultnamehead + 'Rank7.csv')

def calGrayResult(strategyName,symbol,K_MIN,windowsSet,rankpath,rawdatapath):
    '''
    根据排序结果，从月收益表中抽出各月收益，形成推进结果总收益
    :return:
    '''
    #tf = "%s%s_%d_%s" % (rawdatapath, symbol, K_MIN,monthlyfilesuffix)
    #retdf=pd.read_csv(tf,index_col='Setname')
    targetSet=['Rank1','Rank2','Rank3','Rank4','Rank5','Rank6','Rank7']
    #resultlist=[]
    setresultlist=[]
    groupcounter=0
    colss=[]
    for targetName in targetSet:
        for whiteWindows in windowsSet:
            print targetName+' '+str(whiteWindows)
            #每个window进行遍历
            setlist=[groupcounter,targetName,whiteWindows]
            #retlist=[groupcounter,targetName,whiteWindows]
            ranknamehead = ("%s_%s_%s_%d_win%d_" % (rankpath, strategyName,symbol, K_MIN, whiteWindows))
            rankdf=pd.read_csv(ranknamehead+targetName+'.csv',index_col='Setname')  #排名文件
            cols = rankdf.columns.tolist()[1:]
            colss=cols
            for col in cols:
                #按列读取每个月的收益情况
                head=rankdf.sort_values(axis=0,by=col,ascending=False).iloc[0]
                setname=head.name
                setlist.append(setname)
                #ret=retdf.ix[setname,col]
                #retlist.append(ret)
            groupcounter+=1
            setresultlist.append(setlist)
            #resultlist.append(retlist)
    columns=['Group','Target','Windows']
    for c in colss:
        columns.append(c)
    #retresult=pd.DataFrame(resultlist,columns=columns)
    setresultdf= pd.DataFrame(setresultlist,columns=columns)
    #retresult.to_csv(rawdatapath+'ForwardOprAnalyze\\'+symbol+str(K_MIN)+'multiTargetForwardResult.csv')
    setresultdf.to_csv(rawdatapath+'ForwardOprAnalyze\\'+strategyName+' '+symbol+str(K_MIN)+'multiTargetForwardSetname.csv')
    pass

def getOprlistByMonth(strategyName,rawpath,symbol,K_MIN,setname,startmonth,endmonth,columns,filesuffix='result.csv'):
    '''
    根据setname和month，从result结果中取当月的操作集，并返回df
    :param setname:
    :param month:
    :return:
    '''
    closetime_col = columns['closetime_col']
    closeindex_col = columns['closeindex_col']
    closeprice_col = columns['closeprice_col']
    closeutc_col = columns['closeutc_col']
    retr_col = columns['retr_col']
    ret_col = columns['ret_col']

    starttime = startmonth + '-01 00:00:00'
    endtime = endmonth + '-01 00:00:00'
    startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
    endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
    filename=("%s %s%d %s %s"%(strategyName,symbol,K_MIN,setname,filesuffix))
    f=rawpath+filename
    oprdf=pd.read_csv(f)
    oprdf=oprdf.loc[(oprdf['openutc'] >= startutc) & (oprdf['openutc'] < endutc)]
    return oprdf[['opentime','openutc','openindex','openprice',closetime_col,closeutc_col,closeindex_col,closeprice_col,'tradetype',ret_col,retr_col]]

def calOprResult(strategyName,rawpath,symbolinfo,K_MIN,nextmonth,columns,dailyK,positionRatio,initialCash,indexcols,indexcolsFlag,resultfilesuffix='result.csv'):
    '''
    根据灰区的取值，取出各灰区的操作列表，组成目标集组的操作表，并计算各个评价指标
    :return:
    '''
    symbol=symbolinfo.domain_symbol
    graydf=pd.read_csv(rawpath+'ForwardOprAnalyze\\'+strategyName+' '+symbol+str(K_MIN)+'multiTargetForwardSetname.csv',index_col='Group')
    cols = graydf.columns.tolist()[3:]
    cols.append(nextmonth)
    groupResult = []
    closetime_col = columns['closetime_col']
    closeindex_col = columns['closeindex_col']
    closeprice_col = columns['closeprice_col']
    closeutc_col = columns['closeutc_col']
    retr_col = columns['retr_col']
    ret_col = columns['ret_col']
    cash_col = columns['cash_col']
    hands_col = columns['hands_col']

    for i in range(graydf.shape[0]):
        gray=graydf.iloc[i]
        oprdf = pd.DataFrame(columns=['opentime', 'openutc', 'openindex', 'openprice', closetime_col, closeutc_col, closeindex_col,
                     closeprice_col, 'tradetype', ret_col, retr_col])
        print gray.name,gray.Target,gray.Windows
        for l in range(len(cols)-1):
            startmonth=cols[l]
            endmonth=cols[l+1]
            setname=gray[startmonth]
            oprdf=pd.concat([oprdf,getOprlistByMonth(strategyName,rawpath,symbol,K_MIN,setname,startmonth,endmonth,columns,resultfilesuffix)])

        oprdf=oprdf.reset_index(drop=True)

        oprdf['commission_fee'], oprdf['per earn'], oprdf[cash_col], oprdf[hands_col] = RS.calcResult(
            oprdf,
            symbolinfo,
            initialCash,
            positionRatio, ret_col)
        tofilename=('%s %s%d_%s_win%d_oprResult.csv'%(strategyName,symbol,K_MIN,gray.Target,gray.Windows))
        oprdf.to_csv(rawpath+'ForwardOprAnalyze\\'+tofilename)

        dR = RS.dailyReturn(symbolinfo, oprdf, dailyK, initialCash)  # 计算生成每日结果
        dR.calDailyResult()
        tofilename = ('%s %s%d_%s_win%d_oprdailyResult.csv' % (strategyName, symbol, K_MIN, gray.Target, gray.Windows))
        dR.dailyClose.to_csv(rawpath+'ForwardOprAnalyze\\'+tofilename)

        r=RS.getStatisticsResult(oprdf,indexcolsFlag,indexcols,dR.dailyClose)

        groupResult.append([gray.name,gray.Target,gray.Windows]+r)

    groupResultDf=pd.DataFrame(groupResult,columns=['Group','Target','Windows']+indexcols)
    groupResultDf.to_csv(rawpath+'ForwardOprAnalyze\\'+strategyName+' '+symbol+'_'+str(K_MIN)+'_groupOprResult.csv', index=False)
    pass

def getColumnsName(new=True):
    if new:
        return  {
        'closetime_col':'new_closetime',
        'closeutc_col':'new_closeutc',
        'closeindex_col':'new_closeindex',
        'closeprice_col':'new_closeprice',
        'cash_col': 'new_own cash',
        'retr_col': 'new_ret_r',
        'ret_col': 'new_ret',
        'hands_col':'new_hands'
        }
    else:
        return {
        'closetime_col':'closetime',
        'closeutc_col':'closeutc',
        'closeindex_col':'closeindex',
        'closeprice_col':'closeprice',
        'cash_col': 'own cash',
        'retr_col': 'ret_r',
        'ret_col': 'ret',
        'hands_col':'hands'
        }

def runPara(strategyName,whiteWindows,symbolinfo,K_MIN,parasetlist,monthlist,rawdatapath,resultpath,rankpath,columns,filesuffix):
    calWhiteResult(strategyName=strategyName,whiteWindows=whiteWindows, symbolinfo=symbolinfo, K_MIN=K_MIN, parasetlist=parasetlist,
                       monthlist=monthlist, datapath=rawdatapath, resultpath=resultpath,columns=columns,filesuffix=filesuffix)
    rankByWhiteResult(strategyName=strategyName,symbolinfo=symbolinfo, K_MIN=K_MIN, whiteWindows=whiteWindows, datapath=resultpath, resultpath=rankpath)


#def getForward(strategyName,symbolinfo,K_MIN,parasetlist,rawdatapath,startdate,enddate,nextmonth,windowsSet,colslist,positionRatio,initialCash,resultfilesuffix):
def  getMonthParameter(strategyName,startmonth,endmonth,symbolinfo,K_MIN,parasetlist,oprresultpath,columns,resultfilesuffix):
    '''
    根据输出参数计算目标月份应该使用的参数集
    :param month: 目标月份
    :param ranktarget: 评价纬度
    :param windowns: 窗口大小
    :param oprresultpath:
    :param targetpath:
    :return:
    '''
    print ('Calculating month parameters,start from %s to %s' % (startmonth,endmonth))
    symbol=symbolinfo.domain_symbol
    parasetlen = parasetlist.shape[0]
    closeutc_col = columns['closeutc_col']
    retr_col = columns['retr_col']
    ret_col = columns['ret_col']
    cash_col =columns['cash_col']
    annual_list = []
    sharpe_list= []
    success_rate_list = []
    drawback_list = []
    set_list=[]
    for i in np.arange(0, parasetlen):
        setname = parasetlist.ix[i, 'Setname']
        print setname
        filename = oprresultpath + strategyName+' '+symbol + str(K_MIN) + ' ' + setname + resultfilesuffix
        resultdf = pd.read_csv(filename)
        dailydf=pd.read_csv(oprresultpath + strategyName+' '+symbol + str(K_MIN) + ' ' + setname + ' daily'+resultfilesuffix[1:])
        starttime = startmonth+ '-01 00:00:00'
        endtime = endmonth + '-01 00:00:00'
        startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
        endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))
        resultdata = resultdf.loc[(resultdf['openutc'] >= startutc) & (resultdf['openutc'] < endutc)]
        resultdata = resultdata.reset_index(drop=True)
        dailydata = dailydf.loc[(dailydf['utc_time'] >= startutc) & (dailydf['utc_time'] < endutc)]
        #annual_list.append(RS.annual_return(resultdata, cash_col=cash_col, closeutc_col=closeutc_col))
        #sharpe_list.append(RS.sharpe_ratio(resultdata, cash_col=cash_col, closeutc_col=closeutc_col, retr_col=retr_col))
        annual_list.append(RS.annual(dailydata))
        sharpe_list.append(RS.sharpe(dailydata))
        success_rate_list.append(RS.success_rate(resultdata, ret_col=ret_col))
        drawback, a, b = RS.max_drawback(resultdata, cash_col=cash_col)
        drawback_list.append(drawback)
        set_list.append(setname)
        # print('annual:%f.2,sharpe:%f.2,success_rate:%f.2,drawback:%f.2'%(annual,sharpe,success_rate,drawback))
    df = pd.DataFrame(set_list, columns=['Setname'])
    df['Annual']=annual_list
    df['Sharpe']=sharpe_list
    df['SuccessRate']=success_rate_list
    df['DrawBack']=drawback_list

    rangarray = range(df.shape[0], 0, -1)
    df = df.sort_values(by='Annual', ascending=False)
    df['AnnualRank']=0
    df['AnnualRank'] += rangarray
    df = df.sort_values(by='Sharpe', ascending=False)
    df['SharpeRank']=0
    df['SharpeRank'] += rangarray
    df = df.sort_values(by='SuccessRate', ascending=False)
    df['SuccessRank']=0
    df['SuccessRank'] += rangarray
    df = df.sort_values(by='DrawBack', ascending=False)
    df['DrawbackRank']=0
    df['DrawbackRank'] += rangarray

    df = df.sort_values(by='Setname', ascending=True)
    df['Rank1'] = df['AnnualRank']  # 目标集1：年化收益
    df['Rank2'] = df['SharpeRank']  # 目标集2：夏普值
    df['Rank3'] = df['AnnualRank'] * 0.6 + df['DrawbackRank'] * 0.4  # 目标集3：年化收益*0.6+最大回撤*0.4
    df['Rank4'] = df['SharpeRank'] * 0.6 + df['DrawbackRank'] * 0.4  # 目标集4：夏普*0.6+最大回撤*0.4
    df['Rank5'] = df['AnnualRank'] * 0.4 + df['SharpeRank'] * 0.3 + \
                  df['SuccessRank'] * 0.1 + df['DrawbackRank'] * 0.2  # 目标集5：4目标综合
    df['Rank6'] = df['SuccessRank']
    df['Rank7'] = df['SuccessRank'] * 0.5 + df['AnnualRank'] * 0.5
    print ('Calculating month parameters Finished! From %s to %s' % (startmonth, endmonth))
    return df
    #filenamehead = ("%s_%s_%d_%s_parameter" % (targetpath, symbol, K_MIN, endmonth))
    #df.to_csv(filenamehead + '.csv')

    #print datetime.now()

if __name__ == '__main__':
    #参数配置
    exchange_id = 'SHFE'
    sec_id='RB'
    K_MIN = 600
    symbol = '.'.join([exchange_id, sec_id])
    startdate='2016-01-01'
    enddate = '2018-03-01'
    nextmonth='Mar-18'
    #windowsSet=[1,2,3,4,5,6,9,12,15]
    windowsSet=range(1,13)#白区窗口值
    newresult=True
    colslist=getColumnsName(newresult)
    resultfilesuffix='result.csv' #前面不带空格
    #monthlyretrsuffix='monthly_retr.csv' #前面不带下划线

    #文件路径
    upperpath=DC.getUpperPath(uppernume=2)
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN)])
    rawdatapath=resultpath+foldername+'\\'
    parasetlist = pd.read_csv(resultpath+'ParameterOptSet1.csv')
    datapath =resultpath+foldername+'\\'
    forwordresultpath =resultpath+foldername+'\\ForwardResults\\'
    forwardrankpath = resultpath+foldername+'\\ForwardRank\\'


    monthlist = [datetime.strftime(x,'%b-%y') for x in list(pd.date_range(start=startdate, end=enddate,freq='M'))]
    monthlist.append(nextmonth)
    os.chdir(datapath)
    try:
        os.mkdir('ForwardResults')
    except:
        print 'ForwardResults already exist!'
    try:
        os.mkdir('ForwardRank')
    except:
        print 'ForwardRank already exist!'
    try:
        os.mkdir('ForwardOprAnalyze')
    except:
        print 'ForwardOprAnalyze already exist!'

    starttime=datetime.now()
    print starttime

    for whiteWindows in windowsSet:
        #calWhiteResult(whiteWindows=whiteWindows,symbol=symbol,K_MIN=K_MIN,parasetlist=parasetlist,monthlist=monthlist,datapath=rawdatapath,resultpath=forwordresultpath)
        rankByWhiteResult(symbol=symbol,K_MIN=K_MIN,whiteWindows=whiteWindows,datapath=forwordresultpath,resultpath=forwardrankpath)
    
    # 多进程优化，启动一个对应CPU核心数量的进程池

    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)
    l = []
    for whiteWindows in windowsSet:
        l.append(pool.apply_async(runPara,(whiteWindows,symbol,K_MIN,parasetlist,monthlist,rawdatapath,forwordresultpath,forwardrankpath,colslist,resultfilesuffix)))
    pool.close()
    pool.join()

    calGrayResult(symbol, K_MIN, windowsSet, forwardrankpath,rawdatapath)

    calOprResult(rawdatapath,symbol,K_MIN,nextmonth,columns=colslist,resultfilesuffix=resultfilesuffix)
    endtime = datetime.now()
    print starttime
    print endtime