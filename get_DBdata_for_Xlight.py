#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   get_DBdata_for_Xlight.py
@Time    :   2020/05/06 15:40:17
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   从mssql数据库采集数据，处理后导入mysql数据库
'''

# here put the import lib
import mssql_tools as mst
import mysql_tools_class as myc
import common_tools as ct
import datetime as dt
import time
import json
import os
import logging
import csv
import platform
import getopt
import sys
import pandas as pd

logger = logging.getLogger()
ndates = dt.datetime.now().strftime("%Y-%m-%d")


'''
过滤客户通过修改通道后的报单数据
#1,先确认通道修改成功的记录wanping_upload.dbo.t_EQCommand, MdbErrCode=0表示修改成功
#sql1 = "SELECT Content FROM wanping_upload.dbo.t_EQCommand WHERE MdbErrCode = 0"
#1|110010100000003|40750|0100000003|356000012906|5|
#2,Content第一个字段表示交易所，1，上海，2，深圳，第二个字段系统报单编号OrderSysID
#3,根据OrderSysID查询wanping_download.dbo.t_SSEOrder成交数量VolumeTraded等字段
'''
def process_boardOrder(info):
    
    try: 
        mssql_db_ip = info["mssql_db_ip"]
        mssql_user = info["mssql_user"]
        mssql_passwd = info["mssql_passwd"]
        mssql_dl_db = info["mssql_dl_db"]
        mssql_up_db = info["mssql_up_db"]
        mssql_dbname = info["mssql_dbname"]
        # mysql_db_ip = info["mysql_db_ip"]
        # mysql_user = info["mysql_user"]
        # mysql_passwd = info["mysql_passwd"]
        # mysql_dbname = info["mysql_dbname"]
        # mysql_port = info["mysql_port"]

        csv_file_name = './DBdata/BoardOrder/' + ndates +'_BoardOrder_df.csv'
        #先连接upload库查询修改成功的订单
        upDB_info = [mssql_db_ip, mssql_user, mssql_passwd, mssql_up_db]
        downDB_info = [mssql_db_ip, mssql_user, mssql_passwd, mssql_dl_db]
        #(cursor, conn) = mst.connect_mssql(uploaddb_info)
        EQ_sql = "SELECT Content FROM wanping_upload.dbo.t_EQCommand WHERE MdbErrCode = 0"
        content_res,title = mst.get_db_data(EQ_sql, upDB_info)
        logger.info(content_res)
        print("title:",title)
        if content_res != []:
            result_df = pd.DataFrame(columns=['TradingDay','OrderLocalID','OrderSysID','Direction','Price',\
                    'VolumeTotalOriginal','VolumeTraded','SecurityID','ExchangeID','InvestorID','SInfo',\
                    'InvestorName','SecurityName','ShortSecurityName','ExchangeName','DBname'])
            for item in content_res:
                logger.info(item[0])
                cont_list = item[0].split('|')
                print(cont_list)
                #SSEOrder
                if int(cont_list[0]) == 1:
                    ExchangeName = "上海证券交易所"
                    order_sql = "SELECT a.TradingDay,a.OrderLocalID,a.OrderSysID,a.Direction,a.Price,\
                            a.VolumeTotalOriginal,a.VolumeTraded,a.SecurityID,a.ExchangeID,\
                            a.InvestorID,a.SInfo,b.InvestorName,c.SecurityName,c.ShortSecurityName FROM %s.dbo.t_SSEOrder as a \
                            LEFT JOIN %s.dbo.t_Investor as b ON a.InvestorID = b.InvestorID\
                            LEFT JOIN %s.dbo.t_SSESecurity as c ON a.SecurityID = c.SecurityID\
                            WHERE a.OrderSysID = '%s' " % (mssql_dl_db, mssql_dl_db, mssql_dl_db, cont_list[1])
                    #是否为打板
                    #SELECT * FROM t1 LEFT JOIN t2 on t1.cid=t2.id AND t1.name=‘su’
                    #sql3 = "SELECT BoardFlag FROM wanping_download.dbo.t_SSEMarketData WHERE SecurityID = " + order_df[0][2]
                #SZSEOrder
                elif int(cont_list[0]) == 2:
                    print("深圳订单")
                    ExchangeName = "深圳证券交易所"
                    order_sql = "SELECT a.TradingDay,a.OrderLocalID,a.OrderSysID,a.Direction,a.Price,\
                        a.VolumeTotalOriginal,a.VolumeTraded,a.SecurityID,a.ExchangeID,\
                        a.InvestorID,a.SInfo,b.InvestorName,c.SecurityName,c.ShortSecurityName FROM %s.dbo.t_SZSEOrder as a \
                        LEFT JOIN %s.dbo.t_Investor as b ON a.InvestorID = b.InvestorID\
                        LEFT JOIN %s.dbo.t_SZSESecurity as c ON a.SecurityID = c.SecurityID\
                        WHERE a.OrderSysID = '%s' " % (mssql_dl_db, mssql_dl_db, mssql_dl_db, cont_list[1])
                logger.info(order_sql)
                res_df = mst.get_db_df(order_sql, downDB_info)
                res_df['ExchangeName'] = ExchangeName
                res_df['DBname'] = mssql_dbname
                result_df = pd.concat([result_df, res_df], ignore_index=True)
            
            #result_df.to_csv(csv_file_name, encoding='utf-8',index=False)
            result_df.to_csv(csv_file_name, encoding='utf-8')
        
        else:
            logger.info("t_EQCommand没有MdbErrCode等于0的数据!")
        #导入到mysql数据库
        if not os.path.isfile(csv_file_name):           
            logger.error("当天没有BoardOrder订单文件生成")
        else:
            logger.info("导入mysql....")
            info = ct.get_server_config('./config/mysql_config.txt')
            mysql_db_ip = info[0][0]
            mysql_user = info[0][1]
            mysql_passwd = info[0][2]
            mysql_dbname = info[0][3]
            mysql_port = int(info[0][4])
            mysqldb_info = [mysql_db_ip, mysql_user, mysql_passwd,mysql_dbname, mysql_port]
            mysql_obj = myc.mysql_tools(mysqldb_info)
            local_infile_value = mysql_obj.get_local_infile_value()
            #判断mysql参数是否打开允许导入文件
            if local_infile_value == 'ON':
                file_sql = mysql_obj.load_table_commend_gen(csv_file_name, 'fireball_board_order')
                logger.info(file_sql)
                mysql_obj.execute_sql(file_sql)
                logger.info("导入mysql完成")     
            else:
                local_file_msg = "Error，mysql导入csv失败，local_infile 的值为： %s" % local_infile_value
                ct.send_sms_control('NoLimit', local_file_msg, '13681919346')
    except Exception:
        msg = '处理boardOrder数据出现异常'
        logger.error(msg, exc_info=True)
        ct.send_sms_control('NoLimit', msg, '13681919346')
    finally:
        pass

def process_rss_res():
    pass


def main(argv):    
    
    try:       
        yaml_path = './config/get_DBdata_for_X_logger.yaml'
        ct.setup_logging(yaml_path)
        
        with open('./config/get_DBdata_for_X.json', 'r') as f:
            Jsondict = json.load(f)
            logger.debug(Jsondict)      
        
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('sppytraderapi_check.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('python tradeapi_monitor.py -t <task>\n \
                    parameter -t comment: \n \
                    use -t can input the manul single task.\n \
                    task=["board_order","rtt_res"].  \n \
                    task="board_order" means process boarder order  \n \
                    task="rtt_res" means process rtt result')            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
            if manual_task not in ["board_order","rtt_res"]:
                logger.warning("[task] input is wrong, please try again!")
                sys.exit()
            logger.info('manual_task is:%s' % manual_task)
        if manual_task == 'board_order':
            logger.info("Start to excute the board_order data")
            board_order_list = Jsondict['board_order']
            for info in board_order_list:
                process_boardOrder(info)
        elif manual_task == 'rtt_res':
            logger.info("Start to excute the rtt res data")
            process_rss_res()
        else:
            print("Input python XXXX.py -h for help")

    except Exception:
        logger.error('Faild to run get DBdata for X light!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)

           
if __name__ == '__main__':
        main(sys.argv[1:])    


