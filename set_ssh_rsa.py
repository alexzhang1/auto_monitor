#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   set_ssh_rsa.py
@Time    :   2020/08/05 13:57:12
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
import common_tools as ct
import time
import datetime as dt
import os
import logging
import subprocess
#import platform
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')



linuxInfo = ct.get_server_config('./config/ssh_keygen_config_test.csv')


def copy_ssh_rsa(linuxInfo):
    
    logger = logging.getLogger()
    yaml_path = './config/non_trade_monitor_logger.yaml'
    ct.setup_logging(yaml_path)
    error_list = []
    for info in linuxInfo:       
        hostip = info[3]
        username = 'trade'
        password = '123456'
#        servername = info[4]
        command = './copy_ssh_rsa.sh %s %s %s' % (hostip,username,password)

        logger.info(command)
        try:
            res = subprocess.run(command,shell=True,check=True,capture_output=True)
            if res.returncode == 0:
                logger.info("success:" + str(res))
            else:
                logger.error("error:" + str(res))
            logger.info("res.returncode:" + str(res.returncode))
            logger.info("res.stdout:" + res.stdout.decode('utf-8'))
            res_error = res.stderr.decode('utf-8')
            logger.error("res_error:" + res_error)
        except Exception as e:
            res_error = "copy_ssh_rsa 执行异常!" + str(e)
            logger.error("res_error11:" + res_error)
        if res_error:
            logger.error(res_error)
            msg = "Error,服务器[%s]copy_ssh_rsa异常，错误消息：[%s]" % (hostip,res_error)
            logger.error(msg)
            #ct.send_sms_control('NoLimit', msg)
            error_list.append(hostip)
        else:
            # check_list.append(1)
            msg = "Ok,服务器[%s] copy_ssh_rsa 成功" % hostip
            logger.info(msg)
        
        logger.info(error_list)
            
        
    logger.info("copy_ssh_rsa finished")
    for handler in logger.handlers:
        logger.removeHandler(handler)


if __name__ == '__main__':
           
    copy_ssh_rsa(linuxInfo)