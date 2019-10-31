# -*- coding: utf-8 -*-
"""
Created on 2019-07-16 15:36:39

@author: zhangwei
@comment:交易时间监控8:50-15:30
##### 1.进程状态
###### 进程名及参数和进程数量是否匹配，10分钟一次。
##### 2.端口状态
###### 端口存活，10分钟一次。
##### 3.内存状态
###### 使用率是否超过百分之八十，30分钟一次。
##### 4.资金检查
###### 启动时检查一次即可。
##### 5.数据库监控
##### 6.日志监控
###### 先检查确定会有问题的报警类型，先检查部分组件。
##### 7.fpga文件检查
###### fpga文件目录下检查文件报警。
##### 8.关闭短信功能
#####"smss"：查看状态,"sms0"：关闭,"sms100"：打开
##### 9.mdapi行情查询监控
##### 通过api连接后查询行情数据是否正确。
##### 10.traderapi行情查询监控
##### 通过traderapi连接后查询行情数据是否正确。
##### 11.tcp连接数监控
##### 盘中检查tcp连接数是否超限。
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
import monitor_errorLog as mel
import mdapi_monitor as mdt
import traderapi_monitor as tdm
import non_trade_monitor as ntm



logger = logging.getLogger()


#端口和进程监控
def port_process_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.socket_ps_monitor()
        check_result_list = ms.SocPs_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    logger.info("************************The Monitor Result: ************************")
    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "端口和进程监控报警，请查看服务器详细信息!"
        logger.error(msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Port Process Monitor is Worning")
    ct.generate_file(check_result, 'Port_PS_Monitor')


#内存监控
def mem_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.mem_monitor()
        check_result_list = ms.mem_Check_flag_list
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
        msg = "内存监控报警，请查看服务器详细信息"
        logger.error(msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Memory Monitor is Worning")
    ct.generate_file(check_result, 'Memory_Monitor')



#数据库盘中检查
def db_trade_monitor_task():
 
    with open('./config/table_check.json', 'r') as f:
        Jsonlist = json.load(f)
        logger.debug(Jsonlist)
             
#        if os.path.isdir("./tempdata"):
#            for filename in os.listdir('./tempdata'):
#                os.remove('./tempdata/' + filename)
#        else:
#            os.mkdir("./tempdata")
        logger.info("Start to excute the trading_monitor")          
        thrlist = range(len(Jsonlist))
        threads=[]
        for (i,info) in zip(thrlist, Jsonlist):
            t = dbm.MyThread(dbm.trading_monitor,(info,),dbm.trading_monitor.__name__ + str(i))
            threads.append(t)
#                print "thrcouat3:", threading.active_count()
        for i in thrlist:
            threads[i].start()
        for i in thrlist:       
            threads[i].join()
            trading_check = threads[i].get_result()
            sysstr = platform.system()
            if (not trading_check) :
                logger.error("数据库交易中监控报警，请查看服务器详细信息")
                if (sysstr == "Windows") :
                    ct.readTexts("Database trading Worning")


#通过mdapi行情查询检查
def mdapi_monitor_qry_task():
    
    with open('./config/api_monitor_config.json', 'r') as f:
        JsonData = json.load(f)
    
    try:
        res_flag = 0
        for PyMdApi_CheckData in JsonData['PyMdApi']:
#        PyMdApi_CheckData = JsonData['PyMdApi']
            md_test = mdt.mdapi_monitor(PyMdApi_CheckData)
            res = md_test.monitor_market_data()
            res_flag += res
        if res_flag == len(JsonData['PyMdApi']):
            msg = "Ok,所有服务器mdapi行情查询返回结果正确！"
            logger.info(msg)
            ct.send_sms_control("NoLimit", msg)
        else:
            logger.info("Error: 有服务器mdapi行情查询返回结果不正确！")
    except Exception as e:
        msg = str(e)
        logger.error("mdapi monitor 异常：" + msg)



#通过traderapi行情查询检查qry_markat_data
def traderapi_QMD_monitor_task():
    
    with open('./config/api_monitor_config.json', 'r') as f:
        JsonData = json.load(f)
    
    try:
        TraderApi_CheckData = JsonData['PyTraderApi']
        res_flag = 0
        for CheckData in TraderApi_CheckData:                
            check_flag = tdm.run_app("qry_market_data", CheckData)
            res_flag += check_flag
        if res_flag == len(TraderApi_CheckData):
            msg = "Ok,所有服务器 traderapi行情查询 返回结果正确！"
            logger.info(msg)
            ct.send_sms_control("NoLimit", msg)
        else:
            logger.info("Error: 有服务器 traderapi行情查询 返回结果不正确！")
    except Exception as e:
        msg = str(e)
        logger.error("mdapi monitor 异常：" + msg)


#tcp连接数监控
def tcp_connect_monitor_task():

    linuxInfo = ct.get_server_config('./config/tcp_connect_config.txt')
    ntm.common_monitor_task("single_common_monitor", "tcp_connect_info", linuxInfo)



#日志错误监控
def errorLog_monitor_task():
    
    linuxInfo = ct.get_server_config('./config/server_logDir_config.txt')
    ntimes = dt.datetime.now().strftime("%Y%m%d%H%M%S") 
    ndates = dt.datetime.now().strftime("%Y%m%d")      
    #cur_dir_i = os.getcwd()
    cur_dir = os.getcwd().replace("\\","/") + "/"
    log_dir = cur_dir + "mylog/"
    grep_result_file = log_dir + "errorLog_result_" + ndates + '.txt'
    back_file = log_dir + "errorLog_result_" + ntimes + '.txt'
    #备份上一次的grep_result_file文件到back_file
    if (os.path.exists(grep_result_file)):
    	os.rename(grep_result_file, back_file)
        
    grep_lists = mel.get_errorLog(linuxInfo)
    fileNlist = mel.get_result_file_list()
    check_flag = mel.errorLog_check(fileNlist, grep_lists)
    
    sysstr = platform.system()
    if (not check_flag) :
        logger.error("错误日志检查报警，请查看服务器详细信息")
#        send_sms_control("errorLog", "error:Server log error warning")    
        if (sysstr == "Windows"):
            ct.readTexts("Server log warning")


#自己日志监控
def self_log_monitor_task():
    logger.info("self_log_monitor_check msg")
    log_file = './mylog/trade_monitor_run.log'
    with open(log_file, "r") as f:
        lines = f.readlines()
        last_line = lines[-1]
    last_time_str = last_line.split(',')[0]
    #暂停1秒防止一分钟运行2次
    time.sleep(1)
    last_time = dt.datetime.strptime(last_time_str,"%Y-%m-%d %H:%M:%S")
    ntime = dt.datetime.now()
    delta_time = ntime - last_time
    logger.info("delta time is : %d " % delta_time.seconds)
    if delta_time.seconds < 10 and delta_time.seconds >=0 :
        logger.info("ok:I am alive!")
        ct.send_sms_control("NoLimit", "奇点监控服务器自检正常，报警时间点：'10:29','11:59','13:59'")
    else:
        logger.error("error:self check failed")
    
 
#启动程序时清理一下表trade_monitor_pare.json
def init_data():
    
    local_date = dt.datetime.today().strftime('%Y-%m-%d')
    countfile = './config/trade_monitor_para.json'
    with open(countfile, 'r') as f:
        Json_dic = json.load(f)
    if Json_dic["sms_no_control"]["init_day"] != local_date:
        Json_dic["sms_no_control"]["ps_port"] = 0
        Json_dic["sms_no_control"]["mem"] = 0
        Json_dic["sms_no_control"]["fpga"] = 0
        Json_dic["sms_no_control"]["db_init"] = 0
        Json_dic["sms_no_control"]["db_trade"] = 0
        Json_dic["sms_no_control"]["errorLog"] = 0
        Json_dic["sms_no_control"]["total_used_count"] = 0
        Json_dic["sms_no_control"]["init_day"] = local_date
        #单项短信发送次数记录清零
        json_str = json.dumps(Json_dic, indent=4)
        with open(countfile, 'w') as json_file:
            json_file.write(json_str)
        logger.debug("Init para data success")
    else:
        #每天只做一次初始化
        logger.debug("Not to init data")
    



def main(argv):
    
    try:
        yaml_path = './config/trade_monitor_logger.yaml'
        ct.setup_logging(yaml_path)
#        #初始化参数表
#        init_data()
#        #init interval
#        inc = 59
#        #清除tempdate的数据库表记录条数文件。启动时执行一次。
#        if os.path.isdir("./tempdata"):
#            for filename in os.listdir('./tempdata'):
#                os.remove('./tempdata/' + filename)
#        else:
#            os.mkdir("./tempdata")
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('trade_monitor.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('python trade_monitor.py -t <task>\n \
                    parameter -t comment: \n \
                    use -t can input the manul single task.\n \
                    task=["ps_port","mem","fpga","db_init","db_trade","mdapi_qry","traderapi_qmd","errorLog"].  \n \
                    task="ps_port" means porcess and port monitor  \n \
                    task="mem" means memory monitor  \n \
                    task="db_trade" means db trading data monitor  \n \
                    task="errorLog" means file error log monitor  \n \
                    task="mdapi_qry" means mdapi qry market data monitor  \n \
                    task="traderapi_qmd" means mdapi qry market data monitor  \n \
                    task="tcp_con" means tcp connect count monitor  \n \
                    task="self_monitor" means self check monitor  \n \
                    task="smss" means check the sms send status  \n \
                    task="sms0" means set sms total_count=0  \n \
                    task="sms100" means set sms total_count=100  \n \
                    No parameter comment: \n \
                    (default:python trade_monitor.py) means auto work by loops. \n \
                    ps_port_monitor_minites = ["10","20","30","40","50","00"] \n \
                    mem_monitor_minites = ["20","50"] \n \
                    db_monitor_minites = ["26","36","46","56","06","16"] \n \
                    slef_check_mitnits = ["09:00","10:00","11:00","13:00","14:00"] \n \
                    fpga_monitor and db_init_monitor just execute once on beginning ' )            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
            if manual_task not in ["ps_port","mem","fpga","db_init","db_trade","errorLog","mdapi_qry","traderapi_qmd","tcp_con","self_monitor","smss","sms0","sms100"]:
                logger.warning("[task] input is wrong, please try again!")
                sys.exit()
            logger.info('manual_task is:%s' % manual_task)
    #    if inc == 0:
        #task=["ps_port","mem","fpga","db_init","db_trade","errorLog"]
        if manual_task == 'ps_port':
            logger.info("Start to excute the ps_port monitor")
            port_process_task()
        elif manual_task == 'mem':
            logger.info("Start to excute the mem monitor")
            mem_monitor_task()
        elif manual_task == 'self_monitor':
            logger.info("Start to excute the self monitor")
            self_log_monitor_task()
        elif manual_task == 'db_trade':
            logger.info("Start to excute the db_trade monitor")
            db_trade_monitor_task()
        elif manual_task == 'errorLog':
            logger.info("Start to excute the errorLog monitor")
            errorLog_monitor_task()
        elif manual_task == 'mdapi_qry':
            logger.info("Start to excute the mdapi qry_market_data monitor")
            mdapi_monitor_qry_task()
        elif manual_task == 'tcp_con':
            logger.info("Start to excute the tcp connect count monitor")
            tcp_connect_monitor_task()
        elif manual_task == 'traderapi_qmd':
            logger.info("Start to excute the traderapi qry_market_data monitor")
            traderapi_QMD_monitor_task()
        elif manual_task == 'smss':
            logger.info("查看发送短信状态")
            ct.sms_switch('status')
        elif manual_task == 'sms0':
            logger.info("关闭发送短信功能")
            ct.sms_switch(0)
        elif manual_task == 'sms100':
            logger.info("打开发送短信功能")
            ct.sms_switch(100)
        else:
            # 只执行一次的任务，fpga监控，数据库资金等信息监控
            print("Input python trade_monitor_tasks.py -h for help")

    except Exception:
        logger.error('Faild to run trade_monitor_tasks!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)



if __name__ == '__main__':
    main(sys.argv[1:])