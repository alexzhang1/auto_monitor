# -*- coding: utf-8 -*-
"""
Created on 2019-11-14 10:01:58

@author: zhangwei
@comment:linux服务器自动化部署任务
"""

import time
#import threading
import datetime as dt
import common_tools as ct
import platform
import sys
import getopt
import logging
import json
import os


logger = logging.getLogger()


'''
远程对服务器进行对时操作
'''
def ssh_remote_command_task():
    
    error_list = []
    linuxInfo = ct.get_server_config('./config/ssh_remote_command_config.txt')
    for info in linuxInfo:
        hostip = info[0]
        port = int(info[1])
        username = info[2]
        password = info[3]
        command = info[5]

        sshClient = ct.sshConnect(hostip, port, username, password)
        if sshClient == 999:
            msg = "Failed:服务器[%s],连接失败，请检查密码是否正确" % hostip
            logger.error(msg)
            #ct.send_sms_control("NoLimit", msg)
            error_list.append(msg)
        else:
            logger.info("Ok: 服务器[%s]连接正常" % hostip)
            logger.info(hostip + "::" + command)
            #sshRes = ct.sshExecCmd(sshClient, command)
            stdin, stdout, stderr = sshClient.exec_command(command)
            stdoutstr = stdout.read().decode('utf-8')
            ssherr = stderr.read().decode('utf-8')
            if ssherr:
                msg = "服务器[%s]ssh执行命令返回错误：[%s]" % (hostip, ssherr)
                logger.warning(msg)
                error_list.append(msg)
            sshRes = []
            sshRes = stdoutstr.strip().split('\n')
            if sshRes == ['']:
                sshRes = []
            logger.info("sshRes:")
            logger.info(sshRes)
        
        sshClient.close()
                
    if len(error_list) != 0:
        temstr = ';'.join(error_list)
        msg = "Failed:服务器ssh执行命令失败列表：[%s]" % temstr
        logger.error(msg)
        ct.send_sms_control("NoLimit", msg)
    else:
        logger.info("所有服务器ssh执行命令正常！")



def main(argv):
    
    try:
        yaml_path = './config/auto_employment_task_logger.yaml'
        ct.setup_logging(yaml_path)
        #获得log_file目录，不要改变yaml的root设置info_file_handler位置设置，不然获取可能失败
        # t = logger.handlers
        # log_file = t[1].baseFilename
#        print(log_file)
#        log_file = './mylog/non_trade_monitor_run.log'
        #初始化参数表
        ct.init_sms_control_data()
        #init interval
        # inc = 59
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
                    task=["ssh_excute"].  \n \
                    task="sjdr" means sjdr folder Order file monitor  ' )            
                sys.exit()
            elif opt in ("-t", "--task"):
                manual_task = arg
            if manual_task not in ["ssh_excute"]:
                logger.info("[task] input is wrong, please try again!")
                sys.exit()
            logger.info('manual_task is:%s' % manual_task)
    #    if inc == 0:
        #task=["ps_port","mem","fpga","db_init","db_trade","errorLog"]
        if manual_task == 'ping':
            logger.info("Start to excute the ping server monitor")
            ping_monitor_task()
        elif manual_task == 'ssh_excute':
            logger.info("Start to excute the ssh remote command")
            ssh_remote_command_task()
        else:
            # 只执行一次的任务，fpga监控，数据库资金等信息监控
#            fpga_task()
#            db_init_monitor_task()
            print("input error")
            sys.exit()
    except Exception:
        logger.error('Faild to run auto employment task!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)



if __name__ == '__main__':
    main(sys.argv[1:])