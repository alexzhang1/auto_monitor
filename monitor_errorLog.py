# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:01:34 2019

@author: zhangwei
@comment: 通过ssh连接Linux服务器，egrep指令搜索错误日志信息，并写入文件，并报警，报警后，ctrl+c退出程序，再次启动的话将不会报警。
"""

import paramiko
import common_tools as ct
import time
import datetime as dt
import os
import re
import platform
import sys
import getopt
import logging



linuxInfo = ct.get_server_config('./config/server_logDir_config.txt')
#print "linuxInfo:", linuxInfo
ntimes = dt.datetime.now().strftime("%Y%m%d%H%M%S") 
ndates = dt.datetime.now().strftime("%Y%m%d")      
#cur_dir_i = os.getcwd()
cur_dir = os.getcwd().replace("\\","/") + "/"
log_dir = cur_dir + "mylog/"
grep_result_file = log_dir + "errorLog_result_" + ndates + '.txt'
back_file = log_dir + "errorLog_result_" + ntimes + '.txt'
#log_file = log_dir + "montitor_errorLog_run_" + ndates + '.log'
logger = logging.getLogger()



#将监控到的信息写入grep_result_file文件。
def get_errorLog(linuxInfo):
    
    #清空文件内容
    if os.path.exists(grep_result_file):
        with open(grep_result_file, "r+") as f:        
            f.seek(0)
            f.truncate()

    grep_lists = []
    for info in linuxInfo:
      
        hostip = info[0]
        port = info[1]
        username = info[2]
        password = info[3]
        servername = info[4]
        filedir = info[5]
    
        logger.info(servername + "::" + hostip + "get errorLog")
        try:
            ssh=paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostip, port, username, password)
            command = 'egrep -i "error|warning|critical" ' + filedir + ' | egrep -iv "errorid=0|error_code: 4294856287|ERROR woker id"'
            stdin,stdout,stderr = ssh.exec_command(command)
            #egrep -i "error|warning|critical" /home/trade/myproject/log/* | egrep -iv "errorid=0|error_code: 4294856287|ERROR woker id"
            stdoutstr = stdout.read().decode('utf-8')
#            print stdoutstr
            sshRes = []
            sshRes = stdoutstr.strip().split('\n')        
#            for item in templist:
#                 print item.decode('gb2312')                  
#            sshRes = []
#            sshRes = stdout.readlines()
#            print "sshRes:", len(sshRes)
        except Exception as e:
            msg = "SSH and get log failed: [hostip:%s];[username:%s];[error:%s]" % (hostip,username,str(e))
            logger.error(msg)             
        
        for item in sshRes:
#            de_item = item.decode('gb2312')
            error_list = item.strip().split(':', 1)
            grep_lists.append(error_list)
            memstr=','.join(error_list)
            print(memstr)
#            temstr= memstr.encode('utf-8')
            ct.write_file(grep_result_file, memstr)
#        print "grep_lists", grep_lists
    return grep_lists


#取得当天所有的监控文件列表。匹配"errorLog_result_" + ndates 
def get_result_file_list():
    
    ldir = os.listdir(log_dir)
    fileNlist=[]
    for fileN in ldir:
        fullname = log_dir + fileN
        if (os.path.isfile(fullname)):
            todayname = "errorLog_result_" + ndates         
            matchObj = re.match(todayname, fileN, re.M|re.I)
            if (matchObj):
                fileNlist.append(fileN)
    return fileNlist

#filett = 'D:/my_project/python/auto/log_error_result/log_egrep_result_20190527151803.txt'
#count = len(open(filett, 'r').readlines())

#比较上次的文件和这次监控取到的错误记录数 
def errorLog_check(fileNlist, grep_lists):
    
    check_flag = False
    logger.debug('fileNlist: %s', str(fileNlist))
    if len(fileNlist) > 1:
    	#和上一次的文件对比。      
        newfile = grep_result_file
        logger.debug("newfile:", newfile)
        lastfile = fileNlist[-1]
        logger.debug("lastfile", lastfile)
        lastfilecount = len(open(log_dir + lastfile, 'r').readlines())
        logger.info("lastfilecount:%d", lastfilecount)
        newfilecount = len(open(newfile, 'r').readlines())
        logger.info("newfilecount:%d", newfilecount)
        if newfilecount > lastfilecount :
            logger.error("Have New Error log, please check it ")  
            check_flag = False
        else:
            logger.info("Server log is ok ")
            check_flag = True
    elif len(fileNlist) == 1:
        if len(grep_lists) == 2:
            logger.info("Server log is ok ")
            check_flag = True
        else:
            logger.error("Have Error log, please check it ")
            check_flag = False
    else:
        logger.error("Can not get the result file list, please check it ")
        check_flag = False
    
    return check_flag


def monitor_errorLog_run(argv):
    #备份上一次的grep_result_file文件到back_file
    if (os.path.exists(grep_result_file)):
    	os.rename(grep_result_file, back_file)
        
    yaml_path = './config/monitor_errorLog_logger.yaml'
    ct.setup_logging(yaml_path)
    #init interval
    inc = 60
    try:
        opts, args = getopt.getopt(argv,"hl:",["loopsecends="])
    except getopt.GetoptError:
        print('monitor_errorLog.py -l <loopsecends>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('monitor_errorLog.py -l <loopsecends> \n \
                loopsecends=0 means no loop and just run once.\n \
                loopsecends=N means loop interval is N second. \n \
                (default:python monitor_errorLog.py) means loop interval is 60 seconds')          
            sys.exit()
        elif opt in ("-l", "--loopsecends"):
            inc = int(arg)
        print('interval is: ', inc)
    if inc == 0:
        grep_lists = get_errorLog(linuxInfo)
        fileNlist = get_result_file_list()
        check_flag = errorLog_check(fileNlist, grep_lists)
        sysstr = platform.system()
        if (not check_flag) and (sysstr == "Windows"):
            ct.readTexts("Server log warning")
    else:
        while True:
            # 执行方法，函数
            start_time = '05:45'
            end_time = '19:25'
            if (ct.time_check(start_time, end_time)):
                grep_lists = get_errorLog(linuxInfo)
                fileNlist = get_result_file_list()
                check_flag = errorLog_check(fileNlist, grep_lists)
                sysstr = platform.system()
                if (not check_flag) and (sysstr == "Windows"):
                    ct.readTexts("Server log warning")
            time.sleep(inc)


if __name__ == '__main__':          
    monitor_errorLog_run(sys.argv[1:])

