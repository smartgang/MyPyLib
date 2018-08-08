# -*- coding: utf-8 -*-
"""
BIAS1 : (CLOSE-MA(CLOSE,L1))/MA(CLOSE,L1)*100;
BIAS2 : (CLOSE-MA(CLOSE,L2))/MA(CLOSE,L2)*100;
BIAS3 : (CLOSE-MA(CLOSE,L3))/MA(CLOSE,L3)*100;
"""
import pandas as pd


def bias(close, n):
    ma = close.rolling(n).mean()
    bi = (close - ma) / ma * 100
    return bi


if __name__ == '__main__':
    import DATA_CONSTANTS as DC
    domain_symbol = 'SHFE.RB'
    bar_type = 3600
    N = 40
    symbolinfo = DC.SymbolInfo(domain_symbol)
    bar_dic = DC.getBarDic(symbolinfo, bar_type)
    domain_bar = DC.getDomainbarByDomainSymbol(symbolinfo.getSymbolList(), bar_dic, symbolinfo.getSymbolDomainDic())
    domain_bar['bias'] = bias(domain_bar['close'], N)
    domain_bar.to_csv('bias.csv')
    pass
