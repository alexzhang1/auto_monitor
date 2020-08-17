# -*- coding: utf-8 -*-
"""
Created on 2019-07-16 15:36:39

@author: zhangwei
@comment:交易时间监控8:50-15:30


##### 5.数据库监控盘前检查
##### 7.fpga文件检查
###### fpga文件目录下检查文件报警。
"""

import time
#import threading
import datetime as dt
import common_tools as ct
from monitor_server_status import MonitorServer
import platform
import sys
import getopt
import logging
import json
import db_monitor as dbm
import os
#import monitor_errorLog as mel


logger = logging.getLogger()




#fpga文件监控
def fpga_task():

    linuxInfo = ct.get_server_config('./config/fpga_config.txt')
#    linuxInfo = [['192.168.238.7', 22, 'trade', 'trade', 'tradeserver',/home/trade/FPGA']]  
    try:
        ms = MonitorServer(linuxInfo)        
        ms.fpga_monitor_run()
        check_result_list = ms.fpga_Check_flag_list
        logger.debug("check_result_list:")
        logger.debug(check_result_list)
        if len(check_result_list)==0:
            check_result = False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
    except Exception:
        check_result = False
        logger.error('Faild to check fpga!', exc_info=True) 

    logger.info("************************The FPGA File Monitor Result: ************************")
    if check_result:
        logger.info("OK,FPGA Server is OK")
        ct.send_sms_control("fpga", "OK:盘前FPGA文件检查正常")
    else:
        msg = "error:FPGA文件检查异常，请查看详细日志"
        logger.error(msg)
#        send_sms_control("fpga", "error:奇点服务器FPGA文件检查异常，请查看详细日志")
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("VIP Server is Worning")
#        else:
#            ct.send_mail(msg,"Failed_FPGA_Monitor")
#    ct.generate_file(check_result, 'FPAG_File_Monitor')



#数据库盘前检查
def db_init_monitor_task():
    
    with open('./config/table_check.json', 'r') as f:
        Jsonlist = json.load(f)
        logger.debug(Jsonlist)
    
        logger.info("Start to excute the before trade monitor")
        thrlist = range(len(Jsonlist))
        threads=[]
        for (i,info) in zip(thrlist, Jsonlist):
            #print("alltask.__name__:", alltask.__name__)
            t = dbm.MyThread(dbm.before_trade_monitor,(info,),dbm.before_trade_monitor.__name__ + str(i))
            threads.append(t)
            
        for i in thrlist:
            threads[i].start()
        threadResult = []
        for i in thrlist:       
            threads[i].join()
#             threadResult = threads[i].get_result()
#             sysstr = platform.system()
#             if (not threadResult) :
#                 logger.error("error:盘前数据库初始化数据错误，请检查详细错误信息")
# #                send_sms_control("db_init", "error:奇点服务器盘前数据库初始化数据错误，请检查详细错误信息")
#                 if (sysstr == "Windows"):
#                     ct.readTexts("Database init Worning") 
#             else:
#                 logger.info("OK:数据库init检查正常")
#                 ct.send_sms_control("db_init", "OK:盘前数据库init检查正常")
            if (threads[i].get_result()):
                threadResult.append(1)
            else:
                threadResult.append(0)
        check_flag = (sum(threadResult)==len(threadResult))
        if (not check_flag) :
            logger.error("error:有奇点服务器盘前数据库初始化数据错误，请检查详细错误信息")
            logger.error(threadResult)
            #ct.send_sms_control("NoLimit", "error:数据库备份检查失败，请检查详细错误信息")
        else:
            logger.info("OK:盘前数据库init检查正常")
            ct.send_sms_control("db_init", "OK:盘前数据库init检查正常")
                

    

def main(argv):
    
    try:
        yaml_path = './config/trade_monitor_logger.yaml'
        ct.setup_logging(yaml_path)
        #初始化参数表
#        init_data()
        ct.init_sms_control_data()
        #init interval
#        inc = 59
        #清除tempdate的数据库表记录条数文件。启动时执行一次。
        if os.path.isdir("./tempdata"):
            for filename in os.listdir('./tempdata'):
                os.remove('./tempdata/' + filename)
        else:
            os.mkdir("./tempdata")
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('trade_monitor.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('python trade_monitor_init.py -t <task>\n \
                    parameter -t comment: \n \
                    use -t can input the manul single task.\n \
                    task="fpga" means fpga file monitor  \n \
                    task="db_init" means db init data monitor  \n \
                    fpga_monitor and db_init_monitor just execute once on beginning ' )            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
            if manual_task not in ["ps_port","mem","fpga","db_init","db_trade","errorLog"]:
                logger.error("[task] input is wrong, please try again!")
                sys.exit()
            logger.info('manual_task is:%s' % manual_task)
    #    if inc == 0:
        #task=["ps_port","mem","fpga","db_init","db_trade","errorLog"]
        if manual_task == 'fpga':
            logger.info("Start to excute the fpga monitor")
            fpga_task()
        elif manual_task == 'db_init':
            logger.info("Start to excute the db_init monitor")
            db_init_monitor_task()
        else:
            # 只执行一次的任务，fpga监控，数据库资金等信息监控
            #20200728,暂时去掉fpga监控
            #fpga_task()
            db_init_monitor_task()

    except Exception:
        logger.error('Faild to run trade_monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)



if __name__ == '__main__':
    main(sys.argv[1:])