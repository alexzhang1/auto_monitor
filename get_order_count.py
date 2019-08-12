# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 10:33:17 2019

@author: zhangwei
@comment: 统计csv文件中OrderTime列每秒的记录条数，并生成到count_values.csv文件中。
"""

import pandas as pd
import numpy as np
import math
#import sys
import time


def covervalue(x):
    if math.isnan(x):
        return 0
    else:
        return int(x)

def get_count_to_csv(init_file):
    
#    init_file = '20190617_OrderDetail.csv'
    chunker = pd.read_csv(init_file, chunksize=2000000)
    all_count = 0
    tot = pd.Series([])
    for piece in chunker:
    #    print(piece)
        all_count += len(piece)
        print("all_count:", all_count)
        piece.OrderTime = piece.OrderTime.map(lambda x: str(x)[:-3])
#        tot_sub = pd.Series([])
        newdf=piece[['OrderTime','Info2']]
        #gg = newdf['MainSeq'].groupby([newdf['OrderTime'],newdf['MainSeq']]).count()
        gg = newdf.groupby([newdf['OrderTime'],newdf['Info2']]).size()
        if len(tot) == 0:
            tot = gg
        else:
            tot = tot.add(gg, fill_value=0)
    
    print("len(tot):", len(tot))
    
    index_arr = tot.index.to_numpy()
    DF1 = pd.DataFrame({'index_arr': index_arr, 'count': tot.values})
    newDF = pd.DataFrame()
    newDF['OrderTime'] = DF1['index_arr'].map(lambda x: x[0])
    newDF['Info2'] = DF1['index_arr'].map(lambda x: x[1])
    newDF['count'] = DF1['count'].map(lambda x: int(x))
    #print(newDF)
    
    tt1 = pd.date_range(start='2019-06-13 09:30:00',end='2019-06-13 11:30:00',freq='s',normalize=False)
    tt2 = pd.date_range(start='2019-06-13 13:00:00',end='2019-06-13 15:00:00',freq='s',normalize=False)
    tt = tt1.append(tt2)
    #print(tt)
    
    pydate_array = tt.to_pydatetime()
    date_only_array = np.vectorize(lambda s: str(int(s.strftime('%H%M%S'))))(pydate_array )
    date_only_series = pd.Series(date_only_array)
    #print date_only_series
    df2 = pd.DataFrame({'OrderTime': date_only_series.values})
    df2['count']=0
    #DF2 = pd.DataFrame(df2)
    sec_df = pd.DataFrame({'Info2': [1,2,3,4]})
    sec_df['count']=0
    dek_df = pd.merge(df2,sec_df,how='left',on='count')
    
    valuesDF = pd.merge(newDF, dek_df[['OrderTime','Info2']], how='right')
    valuesDF['OrderTime'] = valuesDF['OrderTime'].map(lambda x: int(x))
    valuesDF.set_index(['OrderTime'], inplace=True)
    #print(valuesDF)
    
    valuesDF['count'] = valuesDF['count'].map(lambda x: covervalue(x))
#    valuesDF = valuesDF.sort_values(by='OrderTime', ascending=True)
    valuesDF = valuesDF.sort_values(['OrderTime', 'Info2'], ascending=[True, True])
    print(valuesDF)
    valuesDF.to_csv('count_values.csv', index= True)
    print("Done! Write to file [count_values.csv]")


if __name__ == '__main__':
             
    print('start:%s' %time.ctime())
    init_file = '20190617_OrderDetail.csv'
    get_count_to_csv(init_file)
#    get_count_to_csv(sys.argv[1])
    print('end:%s' %time.ctime())
