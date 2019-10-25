# -*- coding: utf-8 -*-
"""
Created on 2019-07-26 10:01:58

@author: zhangwei
@comment:非交易时间监控
##### 1.物理机存活状态

###### 地址是否可以ping通，一小时一次（16:00-08:00,每个01分执行）。

##### 2.系统存储使用情况

###### 存储占用是否超过百分之八十

##### 3.内存缓存占用情况

###### 缓存是否占用超过百分之五十，超过后可以使用命令清理。

##### 5.进程数量及进程Pid

###### 22:00后trade用户是否有多余进程仍在启动

##### 6.备份文件检查

###### 检查备份当天文件是否存在，大小是否正常。再检查前一天的数据；  
###### 正常的话删除2天前的文件，只保留2天的备份文件。周一删除5天前的文件，周二删除4天前的文件，下午五点执行。

##### 7.download库清库是否完成

###### 凌晨4点检查download库是否清空。除了表dbo.t_transNum

##### 8.查看是否有core文件

###### 使用批量find命令，查看/home/trade/目录下遍历是否有core文件，不删除。定时早上8点和下午4点各执行一次。

##### 9.日初初始化文件是否生成

###### 查看/home/trade/trade_share各节点文件夹内的初始化数据是否存在，及pub席位检查。

##### 10.盘后委托数据是否生成

###### 查看/home/trade/trade_share/sjdr各节点回传数据是否生成。

##### 11.盘后清库检查

###### 检查数据库除了表dbo.t_transNum以外的数据库数据是否被清除。

##### 12.ssh连接检查

###### 通过对服务器的ssh连接，目的是验证密码是否被客户更改掉，或者机器是否连通。

##### 13.跟投费率优惠文件检查

###### 先清除历史文件/home/trade/run/timaker_hx/follow，
###### 远程获取文件home/assess/csvfiles/FollowSecurity_{ndata+1}.csv，
###### 并上传到指定目录/home/trade/run/timaker_hx/follow
###### 校验文件内容是否有重复的记录。
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
import check_xwdm as cx
import remote_file_check as rfc


logger = logging.getLogger()



#ping监控
def ping_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_ping_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.ping_server_monitor()
        check_result_list = ms.ping_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "服务器Ping值异常，请检查详细日志内容！"
        logger.warning(msg)
#        ct.send_sms_control("ping", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Ping Monitor is Worning")



#内存监控
def mem_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.non_trade_mem_monitor()
        check_result_list = ms.mem_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "内存监控报警，请检查详细日志内容！"
        logger.warning(msg)
#        ct.send_sms_control("mem", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Memory Monitor is Worning")
#    ct.generate_file(check_result, 'Memory_Monitor')


#进程监控
def ps_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.non_trade_ps_monitor()
        check_result_list = ms.mem_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "进程监控报警，请检查详细日志内容！"
        logger.warning(msg)
#        ct.send_sms_control("ps_port", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Ps Monitor is Worning")


#磁盘存储监控
def disk_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        ms = MonitorServer(linuxInfo)
        ms.disk_monitor()
        check_result_list = ms.mem_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    if check_result:
        logger.info("All Server is OK")
    else:
        msg = "磁盘使用监控报警，请检查详细日志内容！"
        logger.warning(msg)
#        ct.send_sms_control("disk", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Disk Monitor is Worning")



#core文件监控
def core_file_monitor_task():

    linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    common_monitor_task("single_common_monitor", "core_file_info", linuxInfo)



#公用task任务
def common_monitor_task(task, single_handle, linuxInfo):
    
    #linuxInfo = ct.get_server_config('./config/server_status_config.txt')
    try:
        #single_handle = "core_file_info"
        ms = MonitorServer(linuxInfo, single_handle)
        task_monitor = ms.single_common_monitor
        
        task_monitor()
        check_result_list = ms.common_Check_flag_list
        if len(check_result_list)==0:
            check_result =False
        else:
            check_result = (sum(check_result_list)==len(check_result_list))
#        print "check_result: ", check_result
    except Exception as e:
        check_result = False
        msg = str(e)
        logger.warning(msg)

    if check_result:
        logger.info("All Server is OK")
    else:
        msg = task + "::" + single_handle + " 任务监控报警，请检查详细日志内容！"
        logger.warning(msg)
#        ct.send_sms_control("disk", msg)
        sysstr = platform.system()
        if sysstr == "Windows":
            ct.readTexts("Monitor task is Worning")


'''
初始化csv文件席位代码检查
'''    
def xwdm_monitor_task():

        linuxInfo = ct.get_server_config('./config/check_xwdm_config.txt')
        
        check_flag = 0
        for info in linuxInfo: 
            try:
                hostip = info[0]
                xwdm_check_col = info[6]
                c_x = cx.check_csv_file(info)
                error_list = c_x.check_xwdm()
                print("error_list:", error_list)
                temp_list = []
                for item in error_list:
                    temp_list.append('::'.join(item))
                
                if len(error_list) == 0:
                    logger.info(u"ok:系统 %s 检查成功，客户席位代码都在奇点系统配置内" % hostip)
                    check_flag += 1
                else:
                    if error_list == [['999','999']]:
                        msg = "Error:查询系统 %s席位代码失败，csv文件不存在" % hostip
                        logger.error(msg)
                        ct.send_sms_control('xwdm', msg)
                    else:
                        logger.error(u"Error:检查失败，有客户席位代码不在奇点系统配置内")
                        list_str = ';'.join(temp_list)
                        msg = "Error:系统 %s 检查节点 %s 失败的客户：%s" % (hostip, xwdm_check_col, list_str)
                        logger.error(msg)
                        ct.send_sms_control('xwdm', msg)
            except Exception:
                logger.error("查询席位代码失败，出现异常，请查看服务器日志信息！", exc_info=True)
                ct.send_sms_control('xwdm', "Error:查询席位代码失败，出现异常，请查看服务器日志信息！")

        if check_flag != 0 and check_flag == len(linuxInfo):
            logger.info(u"OK:检查客户席位代码成功!")
        else:
            logger.warning(u"Error:检查客户席位代码失败!")


'''
查看/home/trade/trade_share/sjdr各节点委托回传数据是否生成
'''    
def sjdr_monitor_task():

        linuxInfo = ct.get_server_config('./config/check_sjdr_config.txt')
        
        check_flag = 0
        for info in linuxInfo: 
            try:
                hostip = info[0]
                file_checker = rfc.remote_file_check(info)
                error_list = file_checker.check_sdjr()
                if len(error_list) == 0:
                    msg = "ok:系统 %s 盘后当天的节点委托回传数据检查成功" % hostip
                    logger.info(msg)
                    check_flag += 1
                else:
                    msg = "系统 %s 盘后当天的节点委托回传数据检查失败 失败的文件列表：%s " % (hostip, ';'.join(error_list))
                    logger.error(msg)
                    ct.send_sms_control('NoLimit', msg)
                    
            except Exception:
                logger.error("查询席位代码失败，出现异常，请查看服务器日志信息！", exc_info=True)
                ct.send_sms_control('xwdm', "查询席位代码失败，出现异常，请查看服务器日志信息！")
        if check_flag != 0 and check_flag == len(linuxInfo):
            logger.info(u"OK:检查盘后当天的节点委托回传数据成功!")
        else:
            logger.warning(u"Error:检查盘后当天的节点委托回传数据失败!")

'''
跟投csv文件检查
'''
def follow_monitor_task():
    
        linuxInfo = ct.get_server_config('./config/check_follow_config.txt')
        
        check_flag = 0
        for info in linuxInfo: 
            hostip = info[0]
            try:
                file_checker = rfc.remote_file_check(info)
                res_file = file_checker.check_follow()
                if res_file[:5] != 'Error':
                    msg = "ok:系统 %s 跟投费率优惠文件检查成功，文件名[%s]" % (hostip,res_file)
                    logger.info(msg)
                    check_flag += 1
                    ct.send_sms_control('NoLimit', msg)
                else:
                    msg = "Error:系统 %s 跟投费率优惠文件检查失败 失败的原因：%s" % (hostip, res_file[6:])
                    logger.error(msg)
                    ct.send_sms_control('NoLimit', msg)
                    
            except Exception:
                msg = "Error:系统 %s 跟投费率优惠文件检查失败，出现异常，请查看服务器日志信息！" % hostip
                logger.error(msg, exc_info=True)
                ct.send_sms_control('NoLimit', msg)
        if check_flag != 0 and check_flag == len(linuxInfo):
            logger.info(u"OK:跟投费率优惠文件数据成功!")
        else:
            logger.warning(u"Error:跟投费率优惠文件检查数据失败!")

'''
#盘后数据库清库检查
'''
def cleanup_db_monitor_task():
    
    with open('./config/table_check.json', 'r') as f:
        Jsonlist = json.load(f)
        logger.debug(Jsonlist)
    
        thrlist = range(len(Jsonlist))
        threads=[]
        for (i,info) in zip(thrlist, Jsonlist):
            #print("alltask.__name__:", alltask.__name__)
            t = dbm.MyThread(dbm.cleanup_db_monitor,(info,),dbm.cleanup_db_monitor.__name__ + str(i))
            threads.append(t)
            
        for i in thrlist:
            threads[i].start()
        for i in thrlist:       
            threads[i].join()
            threadResult = threads[i].get_result()
            sysstr = platform.system()
            if (not threadResult) :
                logger.error("error:数据库盘后清库检查失败，请检查详细错误信息")
                ct.send_sms_control("NoLimit", "error:数据库盘后清库检查失败，请检查详细错误信息")
                if (sysstr == "Windows"):
                    ct.readTexts("Database cleanup Worning") 
            else:
                logger.info("OK:数据库盘后清库检查正常")
#                ct.send_sms_control("NoLimit", "OK:数据库盘后清库检查失败检查正常")



#自己日志监控
def self_log_monitor_task(log_file):
    logger.info("self_log_monitor_check msg")
#    log_file = './mylog/non_trade_monitor_run.log'
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
        ct.send_sms_control("NoLimit", "奇点监控服务器自检正常，报警时间点：'18:59','19:59','20:59','21:59',07:59'")
    else:
        logger.error("error:self check failed")
    
 
'''
通过对服务器的ssh连接，目的是验证密码是否被客户更改掉，或者机器是否连通
'''
def check_ssh_connect_task():
    
    error_list = []
    linuxInfo = ct.get_server_config('./config/check_root_passwd_config.txt')
    for info in linuxInfo:
        hostip = info[0]
        port = int(info[1])
        username = info[2]
        password = info[3]
        os_flag = info[4]
        if os_flag == 'l':
            sshClient = ct.sshConnect(hostip, port, username, password)
            if sshClient == 999:
                msg = "Failed:服务器[%s],连接失败，请检查密码是否正确" % hostip
                logger.error(msg)
                #ct.send_sms_control("NoLimit", msg)
                error_list.append(hostip + ":::" + username + ":::" + password)
            else:
                logger.info("Ok: 服务器[%s]连接正常" % hostip)
        else:
            os_info = ct.get_remote_windows_os_info(username, password, hostip)
            if os_info == 'Null':
                msg = "Failed:服务器[%s],连接失败，请检查密码是否正确" % hostip
                logger.error(msg)
                #ct.send_sms_control("NoLimit", msg)
                error_list.append(hostip + ":::" + username + ":::" + password)
            else:
                logger.info("Ok: 服务器[%s]连接正常,操作系统是[%s]" % (hostip,os_info))            
        
    if len(error_list) != 0:
        temstr = ';'.join(error_list)
        msg = "Failed:服务器连接失败列表：[%s]，请检查密码是否正确" % temstr
        logger.error(msg)
        ct.send_sms_control("NoLimit", msg)
    else:
        logger.info("所有服务器连接正常！")
    



def main(argv):
    
    try:
        yaml_path = './config/non_trade_monitor_logger.yaml'
        ct.setup_logging(yaml_path)
        #获得log_file目录，不要改变yaml的root设置info_file_handler位置设置，不然获取可能失败
        t = logger.handlers
        log_file = t[1].baseFilename
#        print(log_file)
#        log_file = './mylog/non_trade_monitor_run.log'
        #初始化参数表
        ct.init_sms_control_data()
        #init interval
        inc = 59
        manual_task = ''
        try:
            opts, args = getopt.getopt(argv,"ht:",["task="])
        except getopt.GetoptError:
            print('non_trade_monitor.py -t <task> or you can use -h for help')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('non_trade_monitor.py -t <task>\n \
                    (default:python non_trade_monitor.py) means auto work by loops. \n \
                    use -t can input the manul single task.\n \
                    task=["ps","mem","ping","disk","core","xwdm","cleanup","sjdr","self_monitor","follow","ssh_connect"].  \n \
                    task="ps" means porcess monitor  \n \
                    task="mem" means memory monitor  \n \
                    task="ping" means ping server monitor  \n \
                    task="disk" means disk monitor  \n \
                    task="core" means core file monitor  \n \
                    task="xwdm" means init VIP_GDH file xwdm check  \n \
                    task="cleanup" means db cleanup check  \n \
                    task="self_monitor" means self check monitor  \n \
                    task="follow" means follow csv file monitor  \n \
                    task="ssh_connect" means ssh connect monitor  \n \
                    task="sjdr" means sjdr folder Order file monitor  ' )            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
            if manual_task not in ["ps","mem","ping","disk","core","xwdm","cleanup","sjdr","follow","self_monitor","ssh_connect"]:
                logger.info("[task] input is wrong, please try again!")
                sys.exit()
            logger.info('manual_task is:%s' % manual_task)
    #    if inc == 0:
        #task=["ps_port","mem","fpga","db_init","db_trade","errorLog"]
        if manual_task == 'ping':
            logger.info("Start to excute the ping server monitor")
            ping_monitor_task()
        elif manual_task == 'mem':
            logger.info("Start to excute the mem monitor")
            mem_monitor_task()
        elif manual_task == 'disk':
            logger.info("Start to excute the disk monitor")
            disk_monitor_task()
        elif manual_task == 'ps':
            logger.info("Start to excute the ps monitor")
            ps_monitor_task()
        elif manual_task == 'core':
            logger.info("Start to excute the core file monitor")
            core_file_monitor_task()
        elif manual_task == 'xwdm':
            logger.info("Start to excute the xwdm check")
            xwdm_monitor_task()
        elif manual_task == 'cleanup':
            logger.info("Start to excute the cleanup db monitor")
            cleanup_db_monitor_task()
        elif manual_task == 'sjdr':
            logger.info("Start to excute the sjdr monitor")
            sjdr_monitor_task()
        elif manual_task == 'follow':
            logger.info("Start to excute the follow csv monitor")
            follow_monitor_task()
        elif manual_task == 'self_monitor':
            logger.info("Start to excute the self monitor")
            self_log_monitor_task(log_file)
        elif manual_task == 'ssh_connect':
            logger.info("Start to excute the ssh login monitor")
            check_ssh_connect_task()
        else:
            # 只执行一次的任务，fpga监控，数据库资金等信息监控
#            fpga_task()
#            db_init_monitor_task()
            print("input error")
            sys.exit()
            #自动监控暂时不做20190814，下面代码无效
            while True:
                     
                start_time = '15:59'
                end_time = '08:30'
                #监控时间点列表
                ps_monitor_minites = ['10','20','30','40','50','00']
#                test_minites = ['12','41','05','07','09','00']          
                mem_monitor_minites = ['20','50']
                db_monitor_minites = ['26','36','46','56','06','16']
                now_Mtime = dt.datetime.now().strftime('%M')
                now_time = dt.datetime.now().strftime('%H:%M')
                if (ct.time_check(start_time, '23:59') or ct.time_check('00:00', end_time)):
                   
                    if (now_time in ['16:00','17:00','21:00','07:00','08:00']):
                        #自己检查自己是否存活
                        self_log_monitor_task(log_file)
                    else:
                        logger.debug("Not to excute self check")
                    #每个01分检查一次。
                    if (now_Mtime == '01'):
                        #10分钟一次，端口和进程监控,错误日志监控
                        ping_monitor_task()
                    else:
                        logger.info("It's not time to excute the ping monitor")
                        
#                    if (now_Mtime in db_monitor_minites) and ct.trade_check():
#                        #10分钟一次，数据库盘中监控
#                        db_trade_monitor_task()
#                    else:
#                        logger.info("It's not time to excute the db monitor")
#    #                now_time = dt.datetime.now().strftime('%H:%M')
                        
                    if (now_Mtime in mem_monitor_minites):
                        #30分钟一次，服务器内存监控
                        mem_monitor_task()
                    else:
                        logger.info("It's not time to excute the mem monitor")            
                    
                    if (ct.time_check('08:00', '15:32')):
                        logger.info("Exit non trade monitor")
                        break
                                   
                else:
                    logger.info("It's not time to excute the non trade monitor")
                time.sleep(inc)
    except Exception:
        logger.error('Faild to run non_trade_monitor!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)



if __name__ == '__main__':
    main(sys.argv[1:])

