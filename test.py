# -*- coding: utf-8 -*-

import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':
    folder = u'D:\\002 MakeLive\实盘明细\推进结果对比\\'
    setname = 'HopeMacdMaWin SHFE.RB3600_Rank6_win4_oprResult'
    oprdf = pd.read_csv(folder+setname+'.csv')
    ret_r = oprdf['new_ret_r']

    arr = ret_r.tolist()

    hist, bin_edges = np.histogram(arr, bins=50)
    width = (bin_edges[1] - bin_edges[0]) * 0.8
    plt.bar(bin_edges[1:], hist*1.0 / sum(hist), width=width, color='#5B9BD5')
    #plt.bar(bin_edges[1:], hist/max(hist))

    cdf = np.cumsum(hist*1.0 / sum(hist))
    plt.plot(bin_edges[1:], cdf, '-*', color='#ED7D31')

    plt.xlim([-0.04, 0.08])
    plt.ylim([0, 1])
    plt.grid()
    plt.title(setname)
    plt.show()