# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:01:34 2019

@author: zhangwei
@comment: 通过ssh连接Linux服务器，实现一些自定义的指令查询，并将结果保存到文件。
"""

import common_tools as ct
import time
import datetime as dt
import os
import logging
#import platform
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')



linuxInfo = ct.get_server_config('./config/normal_query_config.txt')
ntimes = dt.datetime.now().strftime("%Y%m%d%H%M%S") 
ndates = dt.datetime.now().strftime("%Y%m%d")      
cur_dir = os.getcwd().replace("\\","/") + "/"
if os.path.exists("./normal_query"):
    pass
else:
    os.mkdir('./normal_query')
log_dir = cur_dir + "normal_query/"
query_result_file = log_dir + "normal_query_result_" + ntimes + '.txt'
log_file = log_dir + "normal_query_run_log_" + ndates + '.txt'



def get_query_data(linuxInfo):
    
    logger = logging.getLogger()
    yaml_path = './config/non_trade_monitor_logger.yaml'
    ct.setup_logging(yaml_path)
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
        
        sshClient = ct.sshConnect(hostip, port, username, password)
        sshRes = ct.sshExecCmd(sshClient, command)
        logger.info(hostip + "::" + command)
        try:
            for item in sshRes:
#                de_item = item.decode('gb2312')
#                error_list = de_item.strip().split(':', 1)
#                grep_lists.append(error_list)
#                memstr=','.join(error_list)
#                print memstr
#                temstr= item.strip().encode('utf-8')
                temstr= item.strip()
                logger.info(temstr)
                ct.write_file(query_result_file, temstr)
        except Exception as e:
            msg = "write failed: [hostip:%s];[username:%s];[error:%s]" % (hostip,username,str(e))
            logger.error(msg)
            ct.write_log(log_file, msg)
            
        ct.sshClose(sshClient)
    logger.info("get_query_data finished")
    for handler in logger.handlers:
        logger.removeHandler(handler)


if __name__ == '__main__':
           
    get_query_data(linuxInfo)

