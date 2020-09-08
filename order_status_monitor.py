#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   order_status_monitor.py
@Time    :   2020/09/07 09:45:36
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
import mssql_tools as mt
import common_tools as ct
import datetime as dt
import time
import json
import os
import subprocess
import logging
import csv
import threading
import platform
import getopt
import sys
import pandas as pd


logger = logging.getLogger()
ndates = dt.datetime.now().strftime("%Y%m%d")

class MyThread(threading.Thread):

    def __init__(self,func,args,name=''):
        threading.Thread.__init__(self)
        self.name=name
        self.func=func
        self.args=args
    
    def run(self):
        #python3不支持apply
#        apply(self.func,self.args)
        self.result = self.func(*self.args)
        
    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


def order_status_monitor(info):
    
    intv = 5
    i = 0
    tables = ['t_SSEOrder','t_SZSEOrder']
    sh_judge_data = {'pre_list':[], 'cur_list':[], 'error_flag':0}
    szse_judge_data = {'pre_list':[], 'cur_list':[], 'error_flag':0}

    while (i < 5):
        try:               
            server = info["serverip"]
            user = info["user"]
            password = info["password"]
            dbname = info["dbname"]
            sys_type = info["sys_type"]
            upload_dbname = info["upload_dbname"]
    #        servername = info["servername"]           
            db_info = [server, user, password, dbname]            
            #(cursor, conn) = mt.connect_mssql(db_info)
            #"stock,option,credit"
            if sys_type == 'option':
                status = ('0')
            elif sys_type == 'stock':
                status = ('a')
            else:
                status = ('#','0')

            for check_table in tables:
                if check_table == 't_SSEOrder':
                    judge_data = sh_judge_data
                else:
                    judge_data = szse_judge_data
                sql1 = "SELECT [OrderLocalID],[OrderStatus] FROM [%s].[dbo].[%s] where OrderStatus in (%s)" % (dbname, check_table,status)
                sql1 = "SELECT [AccountID] FROM [download].[dbo].[t_FundTransferDetail] where TransferDirection in ('2')"
                logger.info("sql1:" + sql1)
                new_res = mt.get_db_df(sql1,db_info)
                #print("new_res:",new_res)
                if len(new_res) != 0:
                    judge_data['pre_list'] = judge_data['cur_list']
                    judge_data['cur_list'] = list(new_res["AccountID"])
                    com_res = compare_orderlist(judge_data['pre_list'], judge_data['cur_list'])
                    logger.info(com_res)
                    if com_res != 0 and judge_data['error_flag'] < 3 :
                        judge_data['error_flag'] += 1
                        msg = "服务器%s,数据库%s,表%s,订单状态为'0'超过 %d秒" % (server, dbname, check_table, intv)
                        logger.error(msg)
                        #ct.send_sms_control("NoLimit",msg)
                    else:
                        logger.info("订单前后比较结果为0，或者已经报警3次了，不再报警")
                else:
                    logger.info("订单状态没有为'0'的，不需要比较")

                
        except Exception:
            logger.error('Faild to order status check!', exc_info=True)

        finally:
            logger.info("i:" + str(i))
            time.sleep(intv)
            i += 1


def compare_orderlist(pre_list, cur_list):
    Handled_Id = []
    for orderId in pre_list:
        if orderId in cur_list:
            Handled_Id.append(orderId)
    if len(Handled_Id) != 0:
        return ';'.join(Handled_Id)
    else:
        return 0



def main():    
    
    try:       
        yaml_path = './config/orderstatus_check_logger.yaml'
        ct.setup_logging(yaml_path)
        
        with open('./config/table_check.json', 'r') as f:
            Jsonlist = json.load(f)
            logger.debug(Jsonlist)      
        
        #init interval
        thrlist = range(len(Jsonlist))
        threads=[]
        for (i,info) in zip(thrlist, Jsonlist):
            #print("alltask.__name__:", alltask.__name__)
            t = MyThread(order_status_monitor,(info,),order_status_monitor.__name__ + str(i))
            threads.append(t)
            
        for i in thrlist:
            threads[i].start()
        for i in thrlist:       
            threads[i].join()
            #threadResult = threads[i].get_result()      
#            print "thrcount:", threading.active_count() 
                                

    except Exception:
        logger.error('Faild to run monitor db!', exc_info=True)
        ct.send_sms_control("NoLimt", "检查Order表 orderstatus失败！")
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)

           
if __name__ == '__main__':
    main()  
    #test()  