# -*- coding: utf-8 -*-
'''
绘图库
'''
import datetime
import pandas as pd
import numpy
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter, FormatStrFormatter, MultipleLocator

__color_lightsalmon__ = '#ffa07a'
__color_pink__ = '#ffc0cb'
__color_navy__ = '#000080'
__color_gold__ = '#FDDB05'
__color_gray30__ = '0.3'
__color_gray70__ = '0.7'
__color_lightblue__ = 'lightblue'

#======================================================================================================================
#以下图框架准备
_figfacecolor = __color_pink__
_figedgecolor = __color_navy__
_figdpi = 200
_figlinewidth = 1.0
_xfactor = 0.025  # x size * x factor = x length
_yfactor = 0.025  # y size * y factor = y length

_xlength = 8
_ylength = 4

#坐标轴网格====================================================================================
def setup_Axes(axes):
    axes.set_axisbelow(True)  # 网格线放在底层
    #axes.grid(True, 'major', color='0.3', linestyle='solid', linewidth=0.1)
    #axes.grid(True, 'major', color='0.3', linestyle='solid', linewidth=0.1)

#设置X轴=======================================================================================
def setup_xAxis(timeindex,axes,xAxis,xsize,isvisible=False):
    axes.set_xlim(0, xsize)
    xAxis.set_label('price')
    xAxis.set_label_position('top')

    timetmp = []
    for t in timeindex: timetmp.append(t[11:19])
    timelist = [datetime.time(int(hr), int(ms), int(sc)) for hr, ms, sc in
                [dstr.split(':') for dstr in timetmp]]
    i = 0
    minindex = []
    for min in timelist:
        if min.minute % 5 == 0: minindex.append(i)
        i += 1
    xMajorLocator = FixedLocator((numpy.array(minindex)))
    wdindex = numpy.arange(xsize)
    xMinorLocator = FixedLocator(wdindex)

    # 确定 X 轴的 MajorFormatter 和 MinorFormatter
    def x_major_formatter(idx, pos=None):
        if idx<xsize:return timelist[int(idx)].strftime('%H:%M')
        else:return pos

    def x_minor_formatter(idx, pos=None):
        if idx<xsize:return timelist[int(idx)].strftime('%M:%S')
        else:return pos

    xMajorFormatter = FuncFormatter(x_major_formatter)
    xMinorFormatter = FuncFormatter(x_minor_formatter)
    # 设定 X 轴的 Locator 和 Formatter
    xAxis.set_major_locator(xMajorLocator)
    xAxis.set_major_formatter(xMajorFormatter)
    xAxis.set_minor_locator(xMinorLocator)
    xAxis.set_minor_formatter(xMinorFormatter)

    # 设置 X 轴标签的显示样式。
    for mal in axes.get_xticklabels(minor=False):
        mal.set_fontsize(3)
        mal.set_horizontalalignment('center')
        mal.set_rotation('90')
        if isvisible:mal.set_visible(True)
        else: mal.set_visible(False)

    for mil in axes.get_xticklabels(minor=True):
        mil.set_fontsize(3)
        mil.set_horizontalalignment('right')
        mil.set_rotation('90')
        mil.set_visible(False)

#设置Y轴===============================================================================================
def setup_yAxis(axes,yAxis,yhighlim,ylowlim):
    yAxis.set_label_position('left')
    ylimgap = yhighlim - ylowlim
    #   主要坐标点
    # ----------------------------------------------------------------------------
    #        majors = [ylowlim]
    #        while majors[-1] < yhighlim: majors.append(majors[-1] * 1.1)
    majors = numpy.arange(ylowlim, yhighlim, ylimgap / 5)
    minors = numpy.arange(ylowlim, yhighlim, ylimgap / 10)
    #   辅助坐标点
    # ----------------------------------------------------------------------------
    #        minors = [ylowlim * 1.1 ** 0.5]
    #        while minors[-1] < yhighlim: minors.append(minors[-1] * 1.1)
    majorticks = [round(loc,3) for loc in majors if loc > ylowlim and loc < yhighlim]  # 注意，第一项（ylowlim）被排除掉了
    minorticks = [round(loc,3) for loc in minors if loc > ylowlim and loc < yhighlim]

    #   设定 Y 轴坐标的范围
    axes.set_ylim(ylowlim, yhighlim)

    #   设定 Y 轴上的坐标
    #   主要坐标点
    # ----------------------------------------------------------------------------
    yMajorLocator = FixedLocator(numpy.array(majorticks))

    # 确定 Y 轴的 MajorFormatter
    def y_major_formatter(num, pos=None):
        return str(num)

    yMajorFormatter = FuncFormatter(y_major_formatter)

    # 设定 X 轴的 Locator 和 Formatter
    yAxis.set_major_locator(yMajorLocator)
    yAxis.set_major_formatter(yMajorFormatter)

    # 设定 Y 轴主要坐标点与辅助坐标点的样式
    fsize = 4
    for mal in axes.get_yticklabels(minor=False):
        mal.set_fontsize(fsize)

    # 辅助坐标点
    # ----------------------------------------------------------------------------
    yMinorLocator = FixedLocator(numpy.array(minorticks))

    # 确定 Y 轴的 MinorFormatter
    def y_minor_formatter(num, pos=None):
        return str(num)

    yMinorFormatter = FuncFormatter(y_minor_formatter)

    # 设定 Y 轴的 Locator 和 Formatter
    yAxis.set_minor_locator(yMinorLocator)
    yAxis.set_minor_formatter(yMinorFormatter)
    # 设定 Y 轴辅助坐标点的样式
    for mil in axes.get_yticklabels(minor=True):
        mil.set_visible(False)

def setup_yAxisForRet(axes,yAxis):
    axes.set_ylim(-0.03, 0.03)
    yAxis.set_major_locator(MultipleLocator(0.01))
    yAxis.set_major_formatter(FormatStrFormatter('%1.2f') )
    fsize = 4
    for mal in axes.get_yticklabels(minor=False):
        mal.set_fontsize(fsize)



#绘制 K 线===============================================================================================
def drawK(axes,open,high,low,close):
    xindex = numpy.arange(len(high))  # X 轴上的 index，一个辅助数据
    _zipoc = zip(open, close)  # smart:将open和close一对对打包
    up = numpy.array(
        [True if po < pc and po is not None else False for po, pc in _zipoc])  # 标示出该天股价日内上涨的一个序列
    down = numpy.array(
        [True if po > pc and po is not None else False for po, pc in _zipoc])  # 标示出该天股价日内下跌的一个序列
    side = numpy.array(
        [True if po == pc and po is not None else False for po, pc in _zipoc])  # 标示出该天股价日内走平的一个序列

    #   对开收盘价进行视觉修正
    startlist=open.index.tolist()[0]
    for idx, poc in enumerate(_zipoc):
        if poc[0] == poc[1] and None not in poc:
            open[startlist+idx] = poc[0] - 0.1  # 稍微偏离一点，使得在图线上不致于完全看不到
            close[startlist+idx] = poc[1] + 0.1

    rarray_open = numpy.array(open)
    rarray_close = numpy.array(close)
    rarray_high = numpy.array(high)
    rarray_low = numpy.array(low)

    # XXX: 如果 up, down, side 里有一个全部为 False 组成，那么 vlines() 会报错。
    # XXX: 可以使用 alpha 参数调节透明度
    if True in up:
        axes.vlines(xindex[up], rarray_low[up], rarray_high[up], edgecolor='red', linewidth=0.4,
                    label='_nolegend_',
                    alpha=1)
        axes.vlines(xindex[up], rarray_open[up], rarray_close[up], edgecolor='red', linewidth=1.5,
                    label='_nolegend_', alpha=1)

    if True in down:
        axes.vlines(xindex[down], rarray_low[down], rarray_high[down], edgecolor='green', linewidth=0.4,
                    label='_nolegend_', alpha=1)
        axes.vlines(xindex[down], rarray_open[down], rarray_close[down], edgecolor='green', linewidth=1.5,
                    label='_nolegend_', alpha=1)

    if True in side:
        axes.vlines(xindex[side], rarray_low[side], rarray_high[side], edgecolor='0.7', linewidth=0.4,
                    label='_nolegend_', alpha=1)
        axes.vlines(xindex[side], rarray_open[side], rarray_close[side], edgecolor='0.7', linewidth=1.5,
                    label='_nolegend_', alpha=1)

#画MA20===============================================================================
def drawMA(axes,ma,color='white',label='MA'):
    rarray_ma = numpy.array(ma)
    axes.plot(rarray_ma,color=color,linewidth=1,label=label)

#画买卖点：========================================================================================
def drawOprline(axes,opropendf,oprcolsedf,bottom,top_array,beginindex):
    openlongindex=numpy.array(opropendf.loc[opropendf['tradetype']==1]['openindex']-beginindex)
    openshortindex=numpy.array(opropendf.loc[opropendf['tradetype']==-1]['openindex']-beginindex)
    closelongindex=numpy.array(oprcolsedf.loc[oprcolsedf['tradetype']==1]['closeindex']-beginindex)
    closeshortindex=numpy.array(oprcolsedf.loc[oprcolsedf['tradetype']==-1]['closeindex']-beginindex)
    for i in openlongindex: axes.vlines(i,bottom,top_array[int(i)],edgecolor=__color_gold__, linewidth=1,alpha=0.7)
    for i in openshortindex: axes.vlines(i,bottom,top_array[int(i)],edgecolor=__color_navy__, linewidth=1,alpha=0.7)
    for i in closelongindex: axes.vlines(i,bottom,top_array[int(i)],edgecolor=__color_lightblue__, linestyles='dashed',linewidth=1, alpha=0.7)
    for i in closeshortindex: axes.vlines(i,bottom,top_array[int(i)],edgecolor=__color_lightblue__, linestyles='dashed',linewidth=1,alpha=0.7)

#画MACD===============================================================================
def drawMACD(axes,dif,dea,hist):
    rarray_dea = numpy.array(dea)
    rarray_dif = numpy.array(dif)
    axes.plot(rarray_dif,color='white',linewidth=0.5,label='DIF')
    axes.plot(rarray_dea, color='yellow', linewidth=0.5,label='DEA')

#画DMI===============================================================================
def drawDMI(axes,pdi,mdi):
    rarray_pdi = numpy.array(pdi)
    rarray_mdi = numpy.array(mdi)
    axes.plot(rarray_pdi,color='white',linewidth=0.5,label='PDI')
    axes.plot(rarray_mdi, color='yellow', linewidth=0.5,label='MDI')

#画DMI===============================================================================
def drawKDJ(axes,k,d):
    rarray_k = numpy.array(k)
    rarray_d = numpy.array(d)
    axes.plot(rarray_k,color='white',linewidth=0.5,label='KDJ_K')
    axes.plot(rarray_d, color='yellow', linewidth=0.5,label='KDJ_D')



#画收益率=============================================================
#def drawRET_R(axes,longdf,shortdf,beginindex):
def drawRET_R(axes,oprdf,beginindex):
    #生成两个序列，一个序列是值，一个序列是位置
    #longindex = numpy.array(longdf['Unnamed: 0'] - beginindex)
    #shortindex= numpy.array(shortdf['Unnamed: 0'] - beginindex)
    #longvalue = numpy.array(longdf['ret_r'])
    #shortvalue = numpy.array(shortdf['ret_r'])
    longindex=numpy.array(oprdf.loc[oprdf['tradetype']==1]['openindex']-beginindex)
    shortindex = numpy.array(oprdf.loc[oprdf['tradetype'] == -1]['openindex'] - beginindex)
    longvalue = numpy.array(oprdf.loc[oprdf['tradetype'] == 1]['ret_r'])
    shortvalue = numpy.array(oprdf.loc[oprdf['tradetype'] == -1]['ret_r'])
    for i in numpy.arange(len(longindex)):
        value=longvalue[i]
        if value>0:
            axes.vlines(longindex[i],0,value,edgecolor='red', linewidth=3,alpha=0.5)
        else:
            axes.vlines(longindex[i], 0, value, edgecolor='green', linewidth=3, alpha=0.5)
    for i in numpy.arange(len(shortindex)):
        value = shortvalue[i]
        if value > 0:
            axes.vlines(shortindex[i],0,shortvalue[i],edgecolor='red', linewidth=3,alpha=0.5)
        else:
            axes.vlines(shortindex[i], 0, shortvalue[i], edgecolor='green', linewidth=3, alpha=0.5)
    pass

#=============================================
if __name__ == '__main__':
    raw = pd.read_csv('JPG\\CZCE.CF600 Set6(KDJ_N=16,DMI_N=12 ) all.csv')
    raw.index = pd.to_datetime(raw['strtime'])
    raw = raw.tz_localize('PRC')

    openoprraw=pd.read_csv('JPG\\CZCE.CF600 Set6(KDJ_N=16,DMI_N=12 ) result.csv')
    openoprraw.index=pd.to_datetime(openoprraw['opentime'])
    closeoprraw=pd.read_csv('JPG\\CZCE.CF600 Set6(KDJ_N=16,DMI_N=12 )closeopr.csv')
    closeoprraw.index=pd.to_datetime((closeoprraw['closetime']))

    datelist = openoprraw.index.date
    _Fig = plt.figure(figsize=(_xlength, _ylength), dpi=_figdpi,
                      facecolor=_figfacecolor,
                      edgecolor=_figedgecolor, linewidth=_figlinewidth)  # Figure 对象
    axesUp= _Fig.add_axes([0.1, 0.7, 0.8, 0.28], axis_bgcolor='black')
    axesMid1= _Fig.add_axes([0.1, 0.44, 0.8, 0.2], axis_bgcolor='black')
    axesMid2 = _Fig.add_axes([0.1, 0.22, 0.8, 0.2], axis_bgcolor='black')
    axesDown= _Fig.add_axes([0.1, 0.02, 0.8, 0.18], axis_bgcolor='black')


    for od in datelist:
    #三个图公共参数和数据==============================================================================
        oprdate=od.strftime('%Y-%m-%d')
        df=raw[oprdate]
        #longopr=longoprraw[oprdate]
        #shortopr=shortoprraw[oprdate]
        openoprdata=openoprraw[oprdate]
        closeoprdata=closeoprraw[oprdate]
        high = df['high']
        low = df['low']
        open = df['open']
        close = df['close']
        ma5= df['MA_Short']
        ma10 = df['MA_Long']
        _xsize =len(high)
        beginindex = df.ix[df.index[0], 'Unnamed: 0']

    #画上图：K线，买卖点，MA20均线
        print 'drawing K line'
        phigh=max(high.max(),ma5.max(),ma10.max())
        plow=min(low.min(),ma5.min(),ma10.max())
        yhighlimUp = int(phigh / 10) * 10 + 10  # K线子图 Y 轴最大坐标,5%最大波动,调整为10的倍数
        ylowlimUp = int(plow / 10) * 10  # K线子图 Y 轴最小坐标


        xAxisUp = axesUp.get_xaxis()
        yAxisUp = axesUp.get_yaxis()

        axesUp.set_title('K line', fontsize=4)
        setup_Axes(axesUp)
        setup_xAxis(df['strtime'],axesUp,xAxisUp,_xsize,isvisible=True)
        setup_yAxis(axesUp,yAxisUp,yhighlimUp,ylowlimUp)
        drawK(axesUp,open,high,low,close)
        drawMA(axesUp,ma5,color='white')
        drawMA(axesUp,ma10,color='yellow')
        drawOprline(axesUp,openoprdata,closeoprdata,ylowlimUp,numpy.array(low),beginindex)

    #画中1图：DMI线，买卖点
        print 'drawing DMI'
        setup_Axes(axesMid1)
        #axesMid1.set_title('DMI',fontsize=4)
        xAxisMid1=axesMid1.get_xaxis()
        yAxisMid1=axesMid1.get_yaxis()
        setup_xAxis(df['strtime'],axesMid1,xAxisMid1,_xsize)
        yhighlimMid1=max(df['PDI'].max(),df['MDI'].max())
        ylowlimMid1=min(df['PDI'].min(),df['MDI'].min())
        setup_yAxis(axesMid1,yAxisMid1,yhighlimMid1,ylowlimMid1)
        drawDMI(axesMid1,df['PDI'],df['MDI'])
        drawOprline(axesMid1,openoprdata,closeoprdata,ylowlimMid1,numpy.array([yhighlimMid1]*_xsize),beginindex)
        axesMid1.legend(loc='upper right',fontsize=4,shadow=False)

    # 画中2图：KDJ线，买卖点
        print 'drawing KDJ'
        setup_Axes(axesMid2)
        #axesMid2.set_title('KDJ', fontsize=4)
        xAxisMid2 = axesMid2.get_xaxis()
        yAxisMid2 = axesMid2.get_yaxis()
        setup_xAxis(df['strtime'], axesMid2, xAxisMid2, _xsize)
        yhighlimMid2 = max(df['KDJ_K'].max(), df['KDJ_D'].max())
        ylowlimMid2 = min(df['KDJ_K'].min(), df['KDJ_D'].min())
        setup_yAxis(axesMid2, yAxisMid2, yhighlimMid2, ylowlimMid2)
        drawKDJ(axesMid2, df['KDJ_K'], df['KDJ_D'])
        drawOprline(axesMid2, openoprdata,closeoprdata,ylowlimMid2, numpy.array([yhighlimMid2] * _xsize), beginindex)
        axesMid2.legend(loc='upper right', fontsize=4, shadow=False)

    #画下图：买卖收益，在买点画柱状图，y轴是当前开仓的收益率
        print 'drawing ret'
        setup_Axes(axesDown)
        xAxisDown=axesDown.get_xaxis()
        yAxisDown=axesDown.get_yaxis()
        setup_xAxis(df['strtime'],axesDown,xAxisDown,_xsize)
        yhighlimDown=openoprdata['ret_r'].max()
        ylowlimDown=openoprdata['ret_r'].min()
        axesDown.set_title('Return Rate(%)', fontsize=4)
        setup_yAxisForRet(axesDown,yAxisDown)
        drawRET_R(axesDown,openoprdata,beginindex)

        _Fig.savefig('JPG\\'+oprdate+'.png',dip=500)
        axesUp.cla()
        axesMid1.cla()
        axesMid2.cla()
        axesDown.cla()

    #补充没有开仓的日期的数据
    rawdate = pd.DataFrame({'date': raw.index.date})
    a = rawdate.isin(openoprraw.index.date)
    datanotin = rawdate.loc[a[a['date'] == False].index].drop_duplicates()
    for i in datanotin.index:
        # 三个图公共参数和数据==============================================================================
        oprdate = datanotin.loc[i].date.strftime('%Y-%m-%d')
        print oprdate
        df = raw[oprdate]
        openoprdata = openoprraw[oprdate]
        closeoprdata = closeoprraw[oprdate]
        high = df['high']
        low = df['low']
        open = df['open']
        close = df['close']
        ma5 = df['MA_Short']
        ma10 = df['MA_Long']
        _xsize = len(high)
        beginindex = df.ix[df.index[0], 'Unnamed: 0']

        # 画上图：K线，买卖点，MA20均线
        print 'drawing K line'
        phigh = max(high.max(), ma5.max(), ma10.max())
        plow = min(low.min(), ma5.min(), ma10.max())
        yhighlimUp = int(phigh / 10) * 10 + 10  # K线子图 Y 轴最大坐标,5%最大波动,调整为10的倍数
        ylowlimUp = int(plow / 10) * 10  # K线子图 Y 轴最小坐标

        xAxisUp = axesUp.get_xaxis()
        yAxisUp = axesUp.get_yaxis()

        axesUp.set_title('K line', fontsize=4)
        setup_Axes(axesUp)
        setup_xAxis(df['strtime'], axesUp, xAxisUp, _xsize, isvisible=True)
        setup_yAxis(axesUp, yAxisUp, yhighlimUp, ylowlimUp)
        drawK(axesUp, open, high, low, close)
        drawMA(axesUp, ma5, color='white')
        drawMA(axesUp, ma10, color='yellow')
        #drawOprline(axesUp, openoprdata, closeoprdata, ylowlimUp, numpy.array(low), beginindex)

        # 画中1图：DMI线，买卖点
        print 'drawing DMI'
        setup_Axes(axesMid1)
        # axesMid1.set_title('DMI',fontsize=4)
        xAxisMid1 = axesMid1.get_xaxis()
        yAxisMid1 = axesMid1.get_yaxis()
        setup_xAxis(df['strtime'], axesMid1, xAxisMid1, _xsize)
        yhighlimMid1 = max(df['PDI'].max(), df['MDI'].max())
        ylowlimMid1 = min(df['PDI'].min(), df['MDI'].min())
        setup_yAxis(axesMid1, yAxisMid1, yhighlimMid1, ylowlimMid1)
        drawDMI(axesMid1, df['PDI'], df['MDI'])
        #drawOprline(axesMid1, openoprdata, closeoprdata, ylowlimMid1, numpy.array([yhighlimMid1] * _xsize), beginindex)
        axesMid1.legend(loc='upper right', fontsize=4, shadow=False)

        # 画中2图：KDJ线，买卖点
        print 'drawing KDJ'
        setup_Axes(axesMid2)
        # axesMid2.set_title('KDJ', fontsize=4)
        xAxisMid2 = axesMid2.get_xaxis()
        yAxisMid2 = axesMid2.get_yaxis()
        setup_xAxis(df['strtime'], axesMid2, xAxisMid2, _xsize)
        yhighlimMid2 = max(df['KDJ_K'].max(), df['KDJ_D'].max())
        ylowlimMid2 = min(df['KDJ_K'].min(), df['KDJ_D'].min())
        setup_yAxis(axesMid2, yAxisMid2, yhighlimMid2, ylowlimMid2)
        drawKDJ(axesMid2, df['KDJ_K'], df['KDJ_D'])
        #drawOprline(axesMid2, openoprdata, closeoprdata, ylowlimMid2, numpy.array([yhighlimMid2] * _xsize), beginindex)
        axesMid2.legend(loc='upper right', fontsize=4, shadow=False)

        _Fig.savefig('JPG\\' + oprdate + '.png', dip=500)
        axesUp.cla()
        axesMid1.cla()
        axesMid2.cla()
        axesDown.cla()
