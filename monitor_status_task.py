# -*- coding: utf-8 -*-

"""
Created on Wed May 29 14:18:45 2019

@author: zhangwei
@comment: 通过ssh连接Linux服务器，实现了内存，硬盘，端口，线程的监控报警；
实现了fpga文件目录下检查文件报警。
"""


import time
#import threading
import os
import datetime as dt
import common_tools as ct
from monitor_server_status import MonitorServer
import platform
import sys
import getopt
import logging


logger = logging.getLogger()


def alltask():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')

    try:
        ms = MonitorServer(linuxInfo)
        ms.monitor_run()
        check_result_list = ms.Check_flag_list
#        print "check_result_list:", check_result_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.error(msg)

    logger.info("************************The Monitor Result: ************************")
    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "error: Basic Monitor is Worning, Please Check it!"
        logger.error(msg)
#        ct.fortunesms(msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Basic Monitor is Worning")
#        else:
#            ct.send_mail(msg,"Failed_Basic_Monitor")
    ct.generate_file(check_result, 'Basic_Monitor')

def fpgatask():

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
    except Exception as e:
        check_result = False
        logger.error('Faild to check fpga!', exc_info=True) 

    logger.info("************************The FPGA File Monitor Result: ************************")
    if check_result:
        logger.info("FPGA Server is OK")
    else:
        msg = "error:FPGA is Exception, Please Check it!"
        logger.error(msg)
#        ct.fortunesms(msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("VIP Server is Worning")
#        else:
#            ct.send_mail(msg,"Failed_FPGA_Monitor")
    ct.generate_file(check_result, 'FPAG_File_Monitor')


def loop_monitor(argv):
    
    yaml_path = './config/server_status_logger.yaml'
    ct.setup_logging(yaml_path)
    #init interval
    inc = 60
    modul = ''
    try:
        opts, args = getopt.getopt(argv,"hl:e:",["loopsecends=", "excute="])
    except getopt.GetoptError:
        print('monitor_status_task.py -l <loopsecends> -e <excute>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('monitor_status_task.py -l <loopsecends> -e <excute>\n \
                loopsecends=0 means no loop and just run once.\n \
                loopsecends=N means loop interval is N second. \n \
                (default:python monitor_status_task.py) means loop interval is 60 seconds. \n \
                excute=fpga means excute the fpgamonitor. \n \
                excute=basic means excute the basic monitor. \n \
                excute is Null means excute fpga and basic monitor.' )            
            sys.exit()
        elif opt in ("-l", "--loopsecends"):
            inc = int(arg)
        elif opt in ("-e", "--excute"):
            modul = arg
        logger.info('interval is: %d' % inc)
        logger.info('modull is:s: %s' % modul)
    if inc == 0:
        if modul == 'fpga' or modul == '':
            logger.info("Start to excute the fpgamonitor")
            fpgatask()
        if modul == 'basic' or modul == '':
            logger.info("Start to excute the basic monitor")
            alltask()
    else:
        while True:
            # 执行方法，函数
            start_time = '08:45'
            end_time = '19:25'
            if (ct.time_check(start_time, end_time)):
                logger.info("Start to excute the fpgamonitor")
                fpgatask()
            else:
                logger.info("It's not time to excute the FPGA monitor")
            start_time2 = '08:45'
            end_time2 = '15:30'
            if (ct.time_check(start_time2, end_time2)):
                logger.info("Start to excute the basic monitor")
                alltask()
            else:
                logger.info("It's not time to excute the basic monitor")
            time.sleep(inc)



if __name__ == '__main__':
    loop_monitor(sys.argv[1:])