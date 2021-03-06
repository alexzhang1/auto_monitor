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
        logger.error(msg)

    logger.info("************************The Monitor Result: ************************")
    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "端口和进程监控报警，请查看服务器详细信息!"
        logger.error(msg)
#        send_sms_control("ps_port", msg)
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
#        send_sms_control("mem", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Memory Monitor is Worning")
    ct.generate_file(check_result, 'Memory_Monitor')


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
        logger.info("FPGA Server is OK")
        ct.send_sms_control("fpga", "OK:奇点服务器盘前FPGA文件检查正常")
    else:
        msg = "error:奇点服务器FPGA文件检查异常，请查看详细日志"
        logger.error(msg)
#        send_sms_control("fpga", "error:奇点服务器FPGA文件检查异常，请查看详细日志")
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("VIP Server is Worning")
#        else:
#            ct.send_mail(msg,"Failed_FPGA_Monitor")
    ct.generate_file(check_result, 'FPAG_File_Monitor')



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
        for i in thrlist:       
            threads[i].join()
            threadResult = threads[i].get_result()
            sysstr = platform.system()
            if (not threadResult) :
                logger.error("error:奇点服务器盘前数据库初始化数据错误，请检查详细错误信息")
#                ct.send_sms_control("db_init", "error:奇点服务器盘前数据库初始化数据错误，请检查详细错误信息")
                if (sysstr == "Windows"):
                    ct.readTexts("Database init Worning") 
            else:
                logger.info("OK:奇点服务器数据库init检查正常")
                ct.send_sms_control("db_init", "OK:奇点服务器盘前数据库init检查正常")


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
#                send_sms_control("db_trade", "数据库交易中监控报警，请查看服务器详细信息")
                if (sysstr == "Windows") :
                    ct.readTexts("Database trading Worning")


#日志错误监控
def errorLog_monitor_task():
    
    linuxInfo = ct.get_server_config('./config/server_logDir_config.txt')
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
    


'''
短信发送控制，总短信数控制，和单项监控短信控制不超过3次
'''
def send_sms_control(sms_type, msg, phone='13162583883,13681919346'):
    
#    sms_type='ps_port'
#    msg = 'test'
#    phone='13162583883,13681919346'
    
    countfile = './config/trade_monitor_para.json'
    with open(countfile, 'r') as f:
        Json_dic = json.load(f)
        logger.debug(Json_dic)
    total_used_count = Json_dic["sms_no_control"]["total_used_count"]
    total_count = Json_dic["sms_no_control"]["total_count"]
    single_limit = Json_dic["sms_no_control"]["single_limit"]
    total_count = Json_dic["sms_no_control"]["total_count"]
    
#    sms_type = "NoLimi1t"
    if sms_type == "NoLimit":
        single_times = 0
    else:
        try:
            single_times = Json_dic["sms_no_control"][sms_type]
        except Exception as e:
            logger.error(str(e))
            single_times = 999
    logger.info("单项已发送短信次数：%d" % single_times)
    logger.info("已发送短信总条数：%d" % total_used_count)
    #小于限制时才允许发送短信
    if single_times != 999 and total_used_count < total_count and single_times <= single_limit:
#        ct.fortunesms(msg, phone)
        #发送后增加已发送的次数
        count = len(phone.split(','))
        Json_dic["sms_no_control"]["total_used_count"] = total_used_count + count
        Json_dic["sms_no_control"][sms_type] = single_times + 1
        
        json_str = json.dumps(Json_dic, indent=4)
        with open(countfile, 'w') as json_file:
            json_file.write(json_str)
    else:
        logger.error('SMS send count is out of Max value')


def main(argv):
    
    try:
        yaml_path = './config/trade_monitor_logger.yaml'
        ct.setup_logging(yaml_path)
        #初始化参数表
#        init_data()
        ct.init_sms_control_data()
        #init interval
        inc = 59
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
                print('python trade_monitor.py -t <task>\n \
                    parameter -t comment: \n \
                    use -t can input the manul single task.\n \
                    task=["ps_port","mem","fpga","db_init","db_trade","errorLog"].  \n \
                    task="ps_port" means porcess and port monitor  \n \
                    task="mem" means memory monitor  \n \
                    task="fpga" means fpga file monitor  \n \
                    task="db_init" means db init data monitor  \n \
                    task="db_trade" means db trading data monitor  \n \
                    task="errorLog" means file error log monitor  \n \
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
            if manual_task not in ["ps_port","mem","fpga","db_init","db_trade","errorLog","self_monitor","smss","sms0","sms100"]:
                logger.error("[task] input is wrong, please try again!")
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
        elif manual_task == 'fpga':
            logger.info("Start to excute the fpga monitor")
            fpga_task()
        elif manual_task == 'db_init':
            logger.info("Start to excute the db_init monitor")
            db_init_monitor_task()
        elif manual_task == 'db_trade':
            logger.info("Start to excute the db_trade monitor")
            db_trade_monitor_task()
        elif manual_task == 'errorLog':
            logger.info("Start to excute the errorLog monitor")
            errorLog_monitor_task()
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
            #20200728,暂停fpga监控
            #fpga_task()
            db_init_monitor_task()
            while True:                    
                start_time = '08:50'
                end_time = '15:30'
                #监控时间点列表
                ps_monitor_minites = ['10','20','30','40','50','00']
#                test_minites = ['12','41','05','07','09','00']          
                mem_monitor_minites = ['20','50']
                db_monitor_minites = ['26','36','46','56','06','16']
#                now_Htime = dt.datetime.now().strftime('%H')
                now_Mtime = dt.datetime.now().strftime('%M')
                now_time = dt.datetime.now().strftime('%H:%M')
                if (ct.time_check(start_time, end_time)):
                    
                    if (now_time in ['08:59','09:59','10:59','11:59','12:59','13:59','14:59']):
                        #自己检查自己是否存活
                        self_log_monitor_task()
                    else:
                        logger.info("Not to excute self check")
                    
                    if (now_Mtime in ps_monitor_minites):
                        #10分钟一次，端口和进程监控,错误日志监控
                        port_process_task()
                        errorLog_monitor_task()
                    else:
                        logger.info("It's not time to excute the ps monitor")
                        
                    if (now_Mtime in db_monitor_minites) and ct.trade_check():
                        #10分钟一次，数据库盘中监控
                        db_trade_monitor_task()
                    else:
                        logger.info("It's not time to excute the db monitor")
    #                now_time = dt.datetime.now().strftime('%H:%M')
                        
                    if (now_Mtime in mem_monitor_minites):
                        #30分钟一次，服务器内存监控
                        mem_monitor_task()
                    else:
                        logger.info("It's not time to excute the mem monitor")            
                    
                    if (ct.time_check('15:30', '21:32')):
                        logger.info("Exit trade monitor")
                        break
                                   
                else:
                    logger.info("It's not time to excute the trade monitor")
                time.sleep(inc)
    except Exception:
        logger.error('Faild to run trade_monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)




if __name__ == '__main__':
    main(sys.argv[1:])