#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   offer_tcp_monitor.py
@Time    :   2020/06/12 10:51:30
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 11:03:42 2019

@author: zhangwei
@comment: 检查csv文件中xwdm列是否在本系统的配置文件中，config.txt配置文件中xwdm_check_col列为校验列， 
          和csv配置文件列对应不对的将客户报出来。
"""

import paramiko
import datetime as dt
import sys
import csv
#import getopt
import logging
import common_tools as ct
import os
#reload(sys)
#sys.setdefaultencoding('utf-8')


logger = logging.getLogger()


class ssh_command_check:
    
    def __init__(self, info):
        
            self.hostip = info[0]
            self.port = int(info[1])
            self.username = info[2]
            self.password = info[3]
            self.servername = info[4]
            self.tgw_names = info[5]           
            self.exch_front_ip = info[6]
            #获得当天日期字符串
            self.local_date = dt.datetime.today().strftime('%Y-%m-%d')            
            self.sshClient = self.sshConnect()

        
    '''
    创建 ssh 连接函数
    hostip, port, username, password,访问linux的ip，端口，用户名以及密码
    '''
    def sshConnect(self):
        paramiko.util.log_to_file('./mylog/paramiko.log')
        try:
            #创建一个SSH客户端client对象
            sshClient = paramiko.SSHClient()
            # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #创建SSH连接
            sshClient.connect(self.hostip, self.port, self.username, self.password)
            logger.debug("SSH connect success!")
        except Exception as e:
            msg = "SSH connect failed: [hostip:%s];[username:%s];[error:%s]" % (self.hostip, self.username, str(e))
            logger.error(msg)
            ct.send_sms_control('NoLimit',msg)
        return sshClient
    '''
    创建命令执行函数
    command 传入linux运行指令
    '''
    def sshExecCmd(self,command):

        stdin, stdout, stderr = self.sshClient.exec_command(command)
#        if stderr:
#            stderrstr = stderr.read()
#            logger.error(u"exec_command error:" + stderrstr.decode('utf-8'))
#        filesystem_usage = stdout.readlines()
#        return filesystem_usage
#        stdoutstr = stdout.read()           #python2.7不需要decode
        #print(stderr)
        stdoutstr = stdout.read().decode('utf-8')
        sshRes = []
        sshRes = stdoutstr.strip().split('\n')
        return sshRes
    
    '''
    关闭ssh
    '''
    def sshClose(self):
        self.sshClient.close()

    def offer_connect_check(self):
        
        #ndate = self.local_date.replace('-','')
        error_kh_list=[]
        tgw_list = self.tgw_names.split("|")
        #connect_ip = "172.27.128*"
        for tgw_name in tgw_list:
            command = "ss -anp | grep " + self.exch_front_ip + " | grep " + tgw_name
            #command = "ss -anp | grep 192.168.238.7 | grep sshd"
            logger.info("command:" + command)
            sshRes = []
            sshRes = self.sshExecCmd(command)
            logger.info(sshRes)

            if sshRes !=['']:                
                logger.info("服务器%s: 交易网关：[%s] 连接交易所前置地址正确！" % (self.hostip, tgw_name)) 
                error_kh_list.append(1)  
            else:
                msg = "服务器%s: 交易网关：[%s] 连接交易所前置地址不正确，请检查连接状态！" % (self.hostip, tgw_name)
                logger.error(msg) 
                ct.send_sms_control('NoLimit', msg)
                error_kh_list.append(0) 
        self.sshClient.close()
        
        return error_kh_list

        
def main(argv):
  
    
    yaml_path = './config/non_trade_monitor_logger.yaml'
    ct.setup_logging(yaml_path)
    linuxInfo = ct.get_server_config('./config/offer_connect_config.txt')
    
    check_result = []
    for info in linuxInfo: 
        try:
            check_flag = 0
            hostip = info[0]
            tgw_names = info[5]
            scc = ssh_command_check(info)
            error_list = scc.offer_connect_check()
            check_flag = (sum(error_list)==len(error_list))
            if check_flag:
                logger.info(u"ok:系统 %s 交易前置 %s 连接检查成功！" % (hostip,tgw_names))
                check_result.append(1)
            else:
                logger.error(u"系统 %s 交易前置 %s 连接检查失败：" % (hostip,tgw_names))
                check_result.append(0)

        except Exception:
            logger.error('Faild to check offer_connect.', exc_info=True)
            check_result.append(0)
        # finally:
        #     for handler in logger.handlers:
        #         logger.removeHandler(handler)  
    final_check_flag = (sum(check_result)==len(check_result))
    logger.info(check_result)
    if final_check_flag:
        msg = "所有报盘前置连接地址检查成功"
        ct.send_sms_control("NoLimit",msg)
        logger.info(msg)
    else:
        logger.error("报盘连接前置检查有错误！")

    for handler in logger.handlers:
        logger.removeHandler(handler)

      



if __name__ == '__main__':
#    print 'start:%s' %time.ctime()
    main(sys.argv[1:])
#    print 'end:%s' %time.ctime()
