# -*- coding: utf-8 -*-
"""
Created on 2019-08-13 15:17:33

@author: zhangwei
@comment:
    通用远程文件检查类，对远程目录下的文件进行检查。
"""

import csv
import datetime as dt
import sys
import time
#import getopt
import logging
import common_tools as ct
#reload(sys)
#sys.setdefaultencoding('utf-8')


logger = logging.getLogger()


class remote_file_check:
    
    def __init__(self, info):      
        self.hostip = info[0]
        self.port = int(info[1])
        self.username = info[2]
        self.password = info[3]
#        servername = info[4]
        self.remote_dir = info[5]   
        #xwdm检查专用列
        self.xwdm_check_col = info[6]        
        #获得当天日期字符串
        self.local_date = dt.datetime.today().strftime('%Y%m%d')           
        self.sshClient = ct.sshConnect(self.hostip, self.port, self.username, self.password)


    '''
        sdjr检查,取配置文件config/check_sdjr_config.txt'来获取服务器信息，
        比对当天的(t_Order{n}.csv，t_Order{n}.dbf,{ndate}_{n}_end.txt)文件，n=2,3,4,5。
    '''
    def check_sdjr(self):
        
        node_list = [2,3,4,5]
        check_file_list = []
        error_file_list = []
        ndate = self.local_date.replace('-','')
        for node in node_list:
            check_file_list.append(ndate + "_" + str(node) + "_end.txt")
            check_file_list.append("t_Order" + str(node) + ".csv")
            check_file_list.append("t_Order" + str(node) + ".dbf")

        sftp = self.sshClient.open_sftp()
        #先检查end file是否存在
        for file in check_file_list:
            file_path = self.remote_dir + "/" + file
            try:          
                file_mtime = sftp.stat(file_path).st_mtime
                file_mdate = time.strftime("%Y%m%d",time.localtime(file_mtime))
                logger.debug("file_mdate:" + file_mdate)
                if file_mdate == ndate:
                    logger.info("Ok: 服务器 %s 文件 %s 的修改日期为 %s ，当天的节点委托回传数据生成成功!" % (self.hostip, file_path, file_mdate))
                else:
                    msg = "Error: 委托回传数据生成检查失败，服务器 %s 文件 %s 的修改日期为 %s ，与当天机器日期 %s 不符!" % (self.hostip, file_path, file_mdate, ndate)
                    logger.error(msg)
                    error_file_list.append(file)
            except Exception as e:
                msg = "服务器 %s 获取委托回传文件%s 失败，错误信息： %s" % (self.hostip, file_path, str(e))
                logger.error(msg)
                error_file_list.append(file)
        ct.sshClose(self.sshClient)
        return error_file_list

    '''
        xwdm检查,取配置文件config/check_xwdm_config.txt'的字段xwdm_check_col的值选择比较的字段，
        所有的xwdm字段配置在'./config/xwdm_check_list.csv'中，根据上面的字段匹配对应的列，和数据库验证对比。
        不在奇点系统里的客户记录，打印出来。
    '''
    def check_xwdm(self):
        
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
        sshRes = ct.sshExecCmd(self.sshClient, command)
        ct.sshClose(self.sshClient)
#        print "sshRes:", sshRes
        xwdm_list=[]
        for ssr in sshRes[1:]:
            lists = ssr.split(',')
            xwdm_list.append(lists)
            
        error_kh_list = []
        if len(xwdm_list) != 0:
            logger.debug(u"客户席位代码列表为：")
            logger.debug(xwdm_list)           
            for xwdm_item in xwdm_list:
                if (xwdm_item[1] not in check_column):
                    error_kh_list.append(xwdm_item)
        else:
            logger.error(u"error: 没有取到服务器 %s GDH文件 %s，请检查文件路径是否正确！" % (self.hostip, filepath))
            error_kh_list=[[999,999]]
        
        return error_kh_list

        
def main(argv):
    
    #默认是check_sjdr的监控检查
    try:
        yaml_path = './config/check_sjdr_logger.yaml'
        ct.setup_logging(yaml_path)
        linuxInfo = ct.get_server_config('./config/check_sjdr_config.txt')
        
        check_flag = 0
        for info in linuxInfo: 
            hostip = info[0]
            rfc = remote_file_check(info)
            error_list = rfc.check_sdjr()
            if len(error_list) == 0:
                msg = "ok:系统 %s 盘后当天的节点委托回传数据检查成功" % hostip
                logger.info(msg)
                check_flag += 1
            else:
                msg = "系统 %s 盘后当天的节点委托回传数据检查失败 失败的文件列表：%s " % (hostip, ';'.join(error_list))
                logger.error(msg)
                ct.send_sms_control('NoLimit', msg)
                
        if check_flag == len(linuxInfo):
            logger.info(u"OK:检查盘后当天的节点委托回传数据成功!")
        else:
            logger.error(u"Error:检查盘后当天的节点委托回传数据失败!")
    except Exception:
        logger.error("盘后当天的节点委托回传数据出现异常，请参看错误日志！", exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)        



if __name__ == '__main__':
    main(sys.argv[1:])