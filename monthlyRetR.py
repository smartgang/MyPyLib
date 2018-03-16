# -*- coding: utf-8 -*-
'''
多层推进分析第1步：
计算每个组合每个月的独立收益
'''
import pandas as pd
import numpy as np
import DATA_CONSTANTS as DC

def monthyRetR(parasetlist,datapath,symbol,K_MIN,retr_col='ret_r',resultsuffix='result.csv',monthylyresultsuffix='monthly_retr.csv'):
    parasetlen = parasetlist.shape[0]
    prodlist=[]
    for i in np.arange(0, parasetlen):
        setname=parasetlist.ix[i,'Setname']
        print setname
        filename=datapath+symbol + str(K_MIN) + ' ' + setname + ' '+resultsuffix
        result=pd.read_csv(filename)
        result['month'] =result.opentime.str.slice(0, 7)#月是7，天是10
        #print result.month
        result['ret_r_1'] = result[retr_col] + 1
        grouped_ret_r = result['ret_r_1'].groupby(result['month'])
        ret_r_prod = grouped_ret_r.prod()
        ret_r_prod.name=setname
        prodlist.append(ret_r_prod)

    proddf=pd.DataFrame(prodlist)
    #proddf.index.name='Setname'
    tf="%s%s_%d_%s" %(datapath,symbol,K_MIN,monthylyresultsuffix)
    proddf.to_csv(tf)
    return tf

if __name__ == '__main__':
    #参数配置
    exchange_id = 'DCE'
    sec_id='I'
    K_MIN = 600
    symbol = '.'.join([exchange_id, sec_id])
    resultsuffix = 'result.csv'
    monthylyresultsuffix = 'monthly_retr.csv'
    retr_col='ret_r'
    #文件路径
    upperpath=DC.getUpperPath(uppernume=2)
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN)])
    oprresultpath=resultpath+foldername

    parasetlist = pd.read_csv(resultpath+'ParameterOptSet1.csv')
    datapath =resultpath+foldername+'\\'

    monthyRetR(parasetlist,datapath,symbol,K_MIN,retr_col,resultsuffix,monthylyresultsuffix)