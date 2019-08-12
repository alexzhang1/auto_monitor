# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:01:34 2019

@author: zhangwei
@comment: 通过ssh连接Linux服务器，实现一些自定义的指令查询，并将结果保存到文件。
"""

import paramiko
import common_tools as ct
import time
import datetime as dt
import os
#import re
#import platform
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')



linuxInfo = ct.get_server_config('./config/normal_query_config.txt')
#print "linuxInfo:", linuxInfo
ntimes = dt.datetime.now().strftime("%Y%m%d%H%M%S") 
ndates = dt.datetime.now().strftime("%Y%m%d")      
#cur_dir_i = os.getcwd()
cur_dir = os.getcwd().replace("\\","/") + "/"
if os.path.exists("./normal_query"):
    pass
else:
    os.mkdir('./normal_query')
log_dir = cur_dir + "normal_query/"
query_result_file = log_dir + "normal_query_result_" + ntimes + '.txt'

log_file = log_dir + "run_log_" + ndates + '.txt'

'''
creat ssh connect
hostip, port, username, password,访问linux的ip，端口，用户名以及密码
'''
def sshConnect(hostip, port, username, password):
    paramiko.util.log_to_file('paramiko_log')
    try:
        #创建一个SSH客户端client对象
        sshClient = paramiko.SSHClient()
        # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
        sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #创建SSH连接
        sshClient.connect(hostip, port, username, password)
        ct.write_log(log_file,"SSH connect success!")
    except Exception as e:
        msg = "SSH connect failed: [hostip:%s];[username:%s];[error:%s]" %(hostip,username,e)
        print(msg)
        ct.write_log(log_file,msg)
#            exit()
    return sshClient
'''
创建命令执行函数
command 传入linux运行指令
'''
def sshExecCmd(sshClient, command):

    stdin, stdout, stderr = sshClient.exec_command(command)
#    filesystem_usage = stdout.readlines()
    stdoutstr = stdout.read().decode('utf-8')
#    print "stdoutstr:", stdoutstr
#    ssherr = stderr.read()
    ssherr = stderr.read().decode('utf-8')
    if ssherr:
        print("ssherr:", ssherr)
    ct.write_log(log_file, ssherr) 
    sshRes = []
    sshRes = stdoutstr.strip().split('\n')

    return sshRes

'''
关闭ssh
'''
def sshClose(sshClient):
    sshClient.close()


def get_query_data(linuxInfo):
    
    for info in linuxInfo:       
        hostip = info[0]
        port = info[1]
        username = info[2]
        password = info[3]
#        servername = info[4]
        command = info[5]
        
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        temstr= "**********" + cur_time + "::" + hostip + "::" + command + "::**********"
        ct.write_file(query_result_file, temstr)
        
        sshClient = sshConnect(hostip, port, username, password)
        sshRes = sshExecCmd(sshClient, command)
        print(hostip + "::" + command)
        try:
            for item in sshRes:
#                de_item = item.decode('gb2312')
#                error_list = de_item.strip().split(':', 1)
#                grep_lists.append(error_list)
#                memstr=','.join(error_list)
#                print memstr
#                temstr= item.strip().encode('utf-8')
                temstr= item.strip()
                print(temstr)
                ct.write_file(query_result_file, temstr)
        except Exception as e:
            msg = "write failed: [hostip:%s];[username:%s];[error:%s]" % (hostip,username,str(e))
            print(msg)
            ct.write_log(log_file, msg)
            
        sshClose(sshClient)
    print("get_query_data finished")


if __name__ == '__main__':
           
    get_query_data(linuxInfo)

