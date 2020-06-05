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


class check_csv_file:
    
    def __init__(self, info):
        
            self.hostip = info[0]
            self.port = int(info[1])
            self.username = info[2]
            self.password = info[3]
#            servername = info[4]
            self.remote_dir = info[5]           
            self.xwdm_check_col = info[6]
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
        stdoutstr = stdout.read().decode('utf-8')
        sshRes = []
        sshRes = stdoutstr.strip().split('\n')
        return sshRes
    
    '''
    关闭ssh
    '''
    def sshClose(self):
        self.sshClient.close()

    '''
        xwdm检查,取配置文件config/check_xwdm_config.txt'的字段xwdm_check_col的值选择比较的字段，
        所有的xwdm字段配置在'./config/xwdm_check_list.csv'中，根据上面的字段匹配对应的列，和数据库验证对比。
        不在奇点系统里的客户记录，打印出来。
        20200604:修改检查方式：只检查2个文件是否存在
        /home/trade/trade_share/20200604/VIP_INTERFACE_OK20200604.csv
        /home/trade/trade_share/20200604/4/VIP_OK20200604.csv
    '''

    def check_xwdm(self):
        
        ndate = self.local_date.replace('-','')
        vip_interface_filepath = self.remote_dir + '/' + ndate +'/VIP_INTERFACE_OK' + ndate + '.csv'
        vip_ok_filepath = self.remote_dir + '/' + ndate +'/4/VIP_OK' + ndate + '.csv'
        error_kh_list=[]

        for filepath in [vip_interface_filepath, vip_ok_filepath]:
            command = "ls " + filepath
            logger.info("command:" + command)
            sshRes = []
            sshRes = self.sshExecCmd(command)
            # print("sshRes:", sshRes)
            # print(sshRes[0] == filepath)
            # print(sshRes == [''])
            if sshRes !=['']:
                logger.info("服务器%s: 文件：%s存在" % (self.hostip, filepath)) 
                error_kh_list.append(1)  
            else:
                msg = "服务器%s: 文件：%s不存在！" % (self.hostip, filepath)
                logger.error(msg) 
                ct.send_sms_control('xwdm', msg)
                error_kh_list.append(0) 
        self.sshClient.close()
        
        return error_kh_list

    def check_xwdm_old(self):
        
        xwdm_file='./config/xwdm_check_list.csv'
#        self.xwdm_check_col='XWDM_sz'
        with open(xwdm_file,'r') as csvFile:
            reader = csv.DictReader(csvFile)   
            check_column = [row[self.xwdm_check_col] for row in reader]
        logger.info(u"奇点系统 %s 席位号列表为：" % self.hostip)
        logger.info(check_column)
        ndate = self.local_date.replace('-','')
        if self.xwdm_check_col == 'hx_szt':
            filedot='/2'
        elif self.xwdm_check_col == 'hx_shangzt':
            filedot='/3'
        elif self.xwdm_check_col == 'hx_wanping':
            filedot='/4'
        filepath = self.remote_dir + '/' + ndate + filedot + '/VIP_GDH' + ndate + '.csv'
#        filepath = '/home/trade/temp/20190617/VIP_GDH20190403.csv'
        
        #command = 'cat /home/trade/temp/20190617/VIP_GDH20190403.csv | awk -F"' + ',"' + " '{OFS=\",\";print $1,$5}'"
        #cat /home/trade/temp/20190617/VIP_GDH20190403.csv | awk -F"," '{OFS=",";print $1,$5}'
        command = "cat " + filepath + " | awk -F\",\" \'{OFS=\",\";print $1,$5}\'"
        logger.debug("command:" + command)
       
        sshRes = []
        sshRes = self.sshExecCmd(command)
        self.sshClient.close()
#        print "sshRes:", sshRes
        xwdm_list=[]
        for ssr in sshRes[1:]:
            lists = ssr.split(',')
            xwdm_list.append(lists)
            
        error_kh_list = []
        if len(xwdm_list) != 0:
            logger.info(u"客户席位代码列表为：")
            logger.info(xwdm_list)      
            #20191226:修改临时检查，只检查文件存在即可。 
            # for xwdm_item in xwdm_list:
            #     if (xwdm_item[1] not in check_column):
            #         error_kh_list.append(xwdm_item)
        else:
            msg = "error: 没有取到服务器 %s GDH文件 %s，请检查文件路径是否正确！" % (self.hostip, filepath)
            logger.error(msg)
            ct.send_sms_control('xwdm', msg)
            error_kh_list=[['999','999']]
        
        return error_kh_list


        
def main(argv):
  
    try:
        yaml_path = './config/check_xwdm_logger.yaml'
        ct.setup_logging(yaml_path)
        linuxInfo = ct.get_server_config('./config/check_xwdm_config.txt')
        
        
        for info in linuxInfo: 
            check_flag = 0
            hostip = info[0]
            c_x = check_csv_file(info)
            error_list = c_x.check_xwdm()
            check_flag = (sum(error_list)==len(error_list))
            if check_flag:
                logger.info(u"ok:系统 %s 席位代码文件检查成功！" % hostip)
            else:
                logger.error(u"系统 %s 检查席位代码文件失败：" % hostip)
    except Exception:
        logger.error('Faild to check xwdm!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)        



if __name__ == '__main__':
#    print 'start:%s' %time.ctime()
    main(sys.argv[1:])
#    print 'end:%s' %time.ctime()
