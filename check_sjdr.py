# -*- coding: utf-8 -*-
"""
Created on 2019-08-12 14:55:15

@author: zhangwei
@comment:查看/home/trade/trade_share/sjdr各节点委托回传数据是否生成.
"""

import paramiko
import datetime as dt
import sys
import time
#import getopt
import logging
import common_tools as ct
#reload(sys)
#sys.setdefaultencoding('utf-8')


logger = logging.getLogger()


class check_sjdr_file:
    
    def __init__(self, info):
        
        self.hostip = info[0]
        self.port = int(info[1])
        self.username = info[2]
        self.password = info[3]
#        servername = info[4]
        self.remote_dir = info[5]           
        #获得当天日期字符串
        self.local_date = dt.datetime.today().strftime('%Y-%m-%d')            
        self.sshClient = ct.sshConnect(self.hostip, self.port, self.username, self.password)

        
#    '''
#    创建 ssh 连接函数
#    hostip, port, username, password,访问linux的ip，端口，用户名以及密码
#    '''
#    def sshConnect(self):
#        paramiko.util.log_to_file('./mylog/paramiko.log')
#        try:
#            #创建一个SSH客户端client对象
#            sshClient = paramiko.SSHClient()
#            # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
#            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#            #创建SSH连接
#            sshClient.connect(self.hostip, self.port, self.username, self.password)
#            logger.debug("SSH connect success!")
#        except Exception as e:
#            msg = "SSH connect failed: [hostip:%s];[username:%s];[error:%s]" % (self.hostip, self.username, str(e))
#            logger.error(msg)
#        return sshClient
    
#    '''
#    创建命令执行函数
#    command 传入linux运行指令
#    '''
#    def sshExecCmd(self,command):
#
#        stdin, stdout, stderr = self.sshClient.exec_command(command)
##        if stderr:
##            stderrstr = stderr.read()
##            logger.error(u"exec_command error:" + stderrstr.decode('utf-8'))
##        filesystem_usage = stdout.readlines()
##        return filesystem_usage
##        stdoutstr = stdout.read()           #python2.7不需要decode
#        stdoutstr = stdout.read().decode('utf-8')
#        sshRes = []
#        sshRes = stdoutstr.strip().split('\n')
#        return sshRes
#    
#    '''
#    关闭ssh
#    '''
#    def sshClose(self):
#        self.sshClient.close()

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

        
def main(argv):
  
    try:
        yaml_path = './config/check_sjdr_logger.yaml'
        ct.setup_logging(yaml_path)
        linuxInfo = ct.get_server_config('./config/check_sjdr_config.txt')
        
        check_flag = 0
        for info in linuxInfo: 
            hostip = info[0]
            c_x = check_sjdr_file(info)
            error_list = c_x.check_sdjr()
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



