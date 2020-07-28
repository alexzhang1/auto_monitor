# -*- coding: utf-8 -*-
"""
Created on 2020-04-10 11:02:28

@author: zhangwei
@comment:
    rtt_counter python3 verison
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas import DataFrame,Series
from time_recorder import timeit
import datetime as dt
import common_tools as ct
import logging
import os


logger = logging.getLogger()
ndates = dt.datetime.now().strftime("%Y-%m-%d")

@timeit
#def countrtt(path,y,x=None,start=None,end=None,lower=None,upper=None,filter=None,filter_val=None):
def countrtt(path,TradingDay_Order,x=None,y=None,start=None,end=None,lower=0.1,upper=500000,filter=None,filter_val=None):

    if filter:
        df = pd.read_csv(path, usecols=[x, y, filter])
    elif x:
        df = pd.read_csv(path, usecols=[x, y])
    else:
        df = pd.read_csv(path, usecols=y)

    if start and end:
        start = int(start)
        end = int(end)
        df = df[start:end]
    elif start:
        start = int(start)
        df = df[start:]
    elif end:
        end = int(end)
        df = df[:end]

    if filter:
        filter_val = float(filter_val)
        df = df[df[filter] == filter_val]

    #filter num
    if lower:
        lower = float(lower)
        df = df[df[y]>=lower]
    if upper:
        upper = float(upper)
        df = df[df[y]<=upper]

    #wanted num
    '''
    avg = np.mean(df['rtt'])
    min = np.min(df['rtt'])
    max = np.max(df['rtt'])
    var = np.var(df['rtt'])
    idxmin = (df['rtt']).idxmin()
    idxmax = (df['rtt']).idxmax()
    idxmin = Series.idxmin(df['rtt'])
    idxmax = Series.idxmax(df['rtt'])
    median = np.median(df['rtt'])

    #print 'avg',avg
    #print 'min',min,idxmin
    #print 'max',max,idxmax
    #print 'var',var
    #print 'median' ,median
    '''
    # 输出统计数据：平均，中位数，90%等
    msm_str_sub = ''
    csv_df_sub = pd.DataFrame(columns=['TradingDay','NodeID','ExchangeKernel','count','mean','std','min','50%','90%','99%','max'])
    for col_name in y:
        
        desc_df = df.describe(percentiles=[0.5, 0.90, 0.99])
        print("desc_df:",desc_df)
        desc_df[col_name] = desc_df[col_name].map(lambda x: round(float(x),2))
        rtt_df = desc_df[col_name].to_frame()
        db_df = pd.DataFrame(rtt_df.values.T,columns=rtt_df.index)
        db_df.insert(0,'TradingDay',TradingDay_Order)
        db_df.insert(1,'NodeID',path.split('/')[-2])
        if (col_name == 'send_req_rtt'):
            exch_ker_name = path.split('/')[-1][:-14] + 'kernel'
        else:
            exch_ker_name = path.split('/')[-1][:-14] + 'rtt'
        db_df.insert(2,'ExchangeKernel',exch_ker_name)
        tem_dict = desc_df.to_dict()
        if df.empty:
            logger.info("df is empty!")
            msm_str = '[rtt_' + path.split('/')[-2] + '_' + exch_ker_name + ':] count: 0 mean: 0 std: 0;'
        else:
            msm_str = '[rtt_' + path.split('/')[-2] + '_' + exch_ker_name + ':] count:' \
                    + str(tem_dict[col_name]['count']) + ' mean:' + str(tem_dict[col_name]['mean'])\
                    + ' std:' + str(tem_dict[col_name]['std']) + ';'
        logger.info("msm_str:" + msm_str)
        msm_str_sub += msm_str
        csv_df_sub = pd.concat([csv_df_sub, db_df], ignore_index=True)
        #20200515有了数据库文件，暂时不需要之前的落地文件
        # with open(result_file, 'a+') as f:
        #     f.write(rtt_str)
        #     f.write('\n')
    return df, msm_str_sub, csv_df_sub


def draw_scatter(df,x,y,dot_size,pic=None):
    if not dot_size:
        dot_size = 10
    rec_num = len(df)
    if rec_num !=0 :
        point_size = format( float(dot_size)*100 / float(rec_num), '.5f')
    else:
        return

    if x:
        df[[x,y]].plot.scatter(x=x,y=y,s=float(point_size))
    else:
        #pd.DataFrame({'no':np.arange(df.size),'rtt':df[y]}).plot.scatter(x='no',y='rtt',s=float(point_size))
        pd.DataFrame({'no':np.arange(df.size), y:df[y]}).plot.scatter(x='no',y=y,s=float(point_size))

    if pic:
        plt.savefig(pic)
    else:
        plt.show()
    #ts = pd.Series(df[y], index=df[y].index)
  # ts.plot()

@timeit
def tps_count(path,start,end,sample_time=10):
    #num0, num1, num2, num3, dir1):
    '''
    计算TPS要素：采样时间间隔，字符串转时间序列info5，连续处理编号info1
    df = pd.DataFrame({'info5': ['09:30:01', '09:30:03', '09:30:07'], 'info1': [1, 2, 9]})

    Returns:

    '''

    df = pd.read_csv(path)
    if start:
        start = int(start)
        df = df[start:]
    if end:
        end = int(end)
        df = df[:end]

    df = df[['info1', 'info5']]
    df = df[df['info1'] > 0]
    df['info5'] = df['info5'].apply(pd.to_datetime, format='%H:%M:%S')
    ts = df.resample('1S', on='info5').diff()['info1'].resample('1S').pad()

    sample_time = str(sample_time)+'S'
    ts=ts[ts.gt(1000)].resample(sample_time).bfill(1)
    #ts[ts>1000].resample('1S').pad()

    #print df.describe(percentiles=[0.5, 0.90, 0.99])
    return ts

def draw_bar(ts):
    ts.plot.bar()
    plt.show()


def rtt_run(Kfile_dir):
    '''
    说明：
        rtt mode:
        rtt_counter -m rtt -p E:\kernel_rtt.csv -x order_seq_num -y rtt -s 0 -e 500 -l 0 -u 5000 -f rtt5 -v -12 -d 10
        
        tps mode:
        rtt_counter -m tps -p E:\lev2mdtest_rtt2.csv -s 0 -e 500 -t 10
    '''
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('-m', '--mode', help='rtt/tps mode')
    parser.add_option('-n', '--tradeday', help='TradingDay')
    parser.add_option('-p', '--path', help='rtt file path')
    parser.add_option('-x', '--x_axis', help='x axis')
    parser.add_option('-y', '--y_axis', help='y axis')

    # default None
    parser.add_option('-s', '--start', help='start index')
    parser.add_option('-e', '--end', help='end index')
    parser.add_option('-l', '--lower', help='lower limit')
    parser.add_option('-u', '--upper', help='upper limit')
    parser.add_option('-f', '--filter', help='filter name')
    parser.add_option('-v', '--filter_val', help='filter equal value ')
    parser.add_option('-t', '--sample_time', help='tps mode sample time(seconds):default 10')
    parser.add_option('-d', '--dot_size', help='rtt mode scatter draw dot size:default 10')
    parser.add_option('-g','--picture',help='rtt picture path')
    (options, args) = parser.parse_args()
    
    
    #python rtt_counter.py -p ssekernel_rtt.csv -m rtt -y rtt -l 0 -u 500000
    if options.mode == None:
        options.mode = 'rtt'
    kernel_file = ['SSEkernel_rtt.csv','SZSEkernel_rtt.csv']
    sms_msg = ''
    csv_df = pd.DataFrame(columns=['TradingDay','NodeID','ExchangeKernel','count','mean','std','min','percent50','percent90','percent99','max'])
    #获得交易日期，取第一个目录（2号节点）的SSEOrder文件解析TradaingDay
    try:
        order_df = pd.read_csv(Kfile_dir[2] + '/SSEOrder.csv', nrows=2, dtype=object)
        TradingDay_Order = (order_df['TradingDay'][1])
    except Exception as e:
        # logger.error("没有取到交易日期！")
        # print(str(e))
        logger.error("没有取到交易日期！", exc_info=True)
        TradingDay_Order = ndates.replace('-','')
    for f_dir in Kfile_dir:
        for k_file in kernel_file:
            dirname = f_dir.split('/')[-1]
            options.path = f_dir + '/' + k_file
            #options.path = './rtt1/ssekernel_rtt.csv'
            #options.y_axis = 'rtt'
            options.y_axis = ['send_req_rtt','accept_req_rtt']
    
            #print(options)
            if options.mode == 'rtt':
                #print("options.path:",options.path)
                df,msm_str,db_df = countrtt(path=options.path,
                              TradingDay_Order=TradingDay_Order,
                              x=options.x_axis,
                              y=options.y_axis,
                              start=options.start,
                              end=options.end,
                              #lower=options.lower,
                              #upper=options.upper,
                              filter=options.filter,
                              filter_val=options.filter_val)
                sms_msg += msm_str
                db_df.rename(columns={"50%": "percent50", "90%": "percent90","99%": "percent99"},inplace = True)
                csv_df = pd.concat([csv_df, db_df], ignore_index=True)
                for col_name in df.columns:
                    df_draw = df[[col_name]]
                    options.picture='./rtt_result/rtt_' + dirname + '_' + k_file[:-8] + '_' + col_name + '_' + ndates + '.png'
                    draw_scatter(df_draw,x=options.x_axis,y=col_name,dot_size=options.dot_size,pic=options.picture)
            
            elif options.mode == 'tps':
                ts = tps_count(path=options.path,
                               start=options.start,
                               end=options.end,
                               sample_time=options.sample_time)
                draw_bar(ts)
            else:
                raise Exception("mode error")

    print("csv_df:",csv_df)   
    # csv_file_name = './rtt_result/rtt_count_result_' + ndates + '_dbdata.csv'
    # csv_df.to_csv(csv_file_name, encoding='utf-8')
    logger.info("sms_msg:" + sms_msg)
    #20200605暂时不发送短信了，太长需要分段
    #ct.send_sms_control('NoLimit',sms_msg)
    return csv_df



if __name__ == '__main__':
    Kfile_dir = ['./rtt1','./rtt2','./rtt3']
    rtt_run(Kfile_dir)
 #   tps_count()