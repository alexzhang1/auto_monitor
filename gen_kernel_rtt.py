# -*- coding: utf-8 -*-

"""
Created on 2020-04-10 17:18:16

@author: zhangwei
@comment:
    处理kernel_rtt.csv和SSEOrder.csv，SZSEOrder.csv文件
"""
import datetime as dt
import time
import logging
import common_tools as ct
import pandas as pd
import numpy as np
#import math
import rtt_counter as rtt_c
import os
import mysql_tools_class as myc


ndates = dt.datetime.now().strftime("%Y-%m-%d")
logger = logging.getLogger()



def gen_kernel_rtt(file_dir):
    order_files = ['SSEOrder.csv','SZSEOrder.csv']
    kernel = pd.read_csv(file_dir + '/kernel_rtt.csv',low_memory=False)
    #print(kernel['index'])
    kernel.set_index(['index'], inplace=True)
    
    for order_file in order_files:
        #order_df = pd.read_csv(file_dir + '/' + order_file,low_memory=False,encoding='latin1')
        order_df = pd.read_csv(file_dir + '/' + order_file,low_memory=False,encoding='iso-8859-1')
        #print(order_df['MeasureIndex'])
        order_df['index'] = order_df['MeasureIndex']
        order_df.set_index(['MeasureIndex'], inplace=True)
        #print(order_df)
            
        order_kernel = pd.merge(kernel,order_df['index'], on='index')
        #sec_str = order_file.split('Order')[0].lower()
        sec_str = order_file.split('Order')[0]
        order_kernel.to_csv(file_dir + '/' + sec_str + 'kernel_rtt.csv', index= False)

def main():
  
    try:
        yaml_path = './config/gen_rtt_logger.yaml'
        ct.setup_logging(yaml_path)
#        #复制文件
#        linuxInfo = ct.get_server_config('./config/rtt_transfer_config.txt')
#        
#        #读取输入参数
#        for info in linuxInfo:               
#            hostip = info[0]
#            port = int(info[1])
#            username = info[2]
#            password = info[3]
#            local_upload = info[5]
#            local_download = info[6]
#            remote_dir = info[7]
#            cp_files = ['SSEOrder.csv','SZSEOrder.csv','kernel_rtt.csv']
#            for singlefile in cp_files:
#                ft.download(local_download, remote_dir, hostip, port, username, password, singlefile)

        #rtt_run
        Kfile_dir = ['/home/trade/rtt/2','/home/trade/rtt/3','/home/trade/rtt/4','/home/trade/rtt/5','/home/trade/rtt/7']
        #Kfile_dir = ['./rtt_result/2','./rtt_result/3'] #test
        for file_dir in Kfile_dir:
            gen_kernel_rtt(file_dir)
        logger.info("gen rtt file finished")
        csv_df = rtt_c.rtt_run(Kfile_dir)
        #csv_df = pd.DataFrame(columns=['TradingDay','NodeID','ExchangeKernel','count','mean','std','min','percent50','percent90','percent99','max'])
        csv_file_name = './rtt_result/rtt_count_result_' + ndates + '_dbdata.csv'
        csv_df.to_csv(csv_file_name, encoding='utf-8')
        if not os.path.isfile(csv_file_name):           
            logger.error("当天没有rtt_result文件生成")
        else:
            logger.info("导入mysql....")
            info = ct.get_server_config('./config/mysql_config.txt')
            mysql_db_ip = info[0][0]
            mysql_user = info[0][1]
            mysql_passwd = info[0][2]
            mysql_dbname = info[0][3]
            mysql_port = int(info[0][4])
            mysqldb_info = [mysql_db_ip, mysql_user, mysql_passwd,mysql_dbname, mysql_port]
            #mysqldb_info = ["192.168.238.21","test","test123","test_db", 3306]
            #
            mysql_obj = myc.mysql_tools(mysqldb_info)
            file_sql = mysql_obj.load_table_commend_gen(csv_file_name, 'rtt_kernel_count')
            logger.info(file_sql)
            mysql_obj.execute_sql(file_sql)   
        
    except Exception:
        msg = "处理rrt_result数据出现异常"
        logger.error(msg, exc_info=True)
        ct.send_sms_control('NoLimit', msg, '13681919346')
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)




if __name__ == '__main__':
    print('start:%s' %time.ctime())
    main()
    print('end:%s' %time.ctime())