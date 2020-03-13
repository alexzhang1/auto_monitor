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
import paramiko
import stat
import re
import pandas as pd
import os
import subprocess
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
        if info[5]:
            self.remote_dir = info[5]   
        #xwdm检查专用列
        if info[6]:
            self.xwdm_check_col = info[6]  
        if info[7]:
            self.follow_hostip = info[7]
        if info[8]:
            self.follow_port = int(info[8])
        if info[9]:
            self.follow_username = info[9]
        if info[10]:
            self.follow_password = info[10]
        if info[11]:
            self.follow_dir = info[11]
        #获得当天日期字符串
        self.local_date = dt.datetime.today().strftime('%Y%m%d')
        self.next_date = (dt.datetime.today()+dt.timedelta(1)).strftime('%Y%m%d')
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
        logger.debug(command)
       
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



    # ------获取远端linux主机上的文件是否是文件夹
    def get_remote_isdir(self, sftp, path):
        try:
            return stat.S_ISDIR(sftp.stat(path).st_mode)
        except IOError:
            return False

    #递归删除目录下所有文件及文件夹，不包含自己目录
    def rm_remote_dir(self, sftp, path):
        files = sftp.listdir(path=path)
    
        for f in files:
            filepath = path + '/' + f
            if self.get_remote_isdir(sftp, filepath):
                self.rm_remote_dir(sftp, filepath)
            else:
                #print 'filepath:',  filepath
                sftp.remove(filepath)    
#        sftp.rmdir(path)



    '''
    ###### 先清除历史文件/home/trade/run/timaker_hx/follow，
    ###### 远程获取文件home/assess/csvfiles/{ndate+1}*.csv，
    ###### 并上传到指定目录/home/trade/run/timaker_hx/follow
    ###### 校验文件内容是否有重复的记录。
    '''
    def check_follow(self):
        
        #先清除历史文件/home/trade/run/timaker_hx/follow
        t = paramiko.Transport((self.hostip, self.port))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        self.rm_remote_dir(sftp, self.remote_dir)
        #上传跟投文件到服务器
        #scp trade@192.168.238.7:/home/trade/csvfiles/FollowSecurity_YYYYMMDD.csv /home/trade/run/timaker_hx/follow        
        #command = "scp " + self.follow_username + "@" + self.follow_hostip + ":" + self.follow_dir + self.next_date + "* " + self.remote_dir
        #command = "scp " + self.follow_username + "@" + self.follow_hostip + ":" + self.follow_dir + "FollowSecurity_" + "\*$" + self.next_date + ".csv " + self.remote_dir
        command = "scp " + self.follow_username + "@" + self.follow_hostip + ":" + self.follow_dir + "FollowSecurity_" + self.next_date + ".csv " + self.remote_dir
        logger.info(command)
        sshRes = ct.sshExecCmd(self.sshClient, command)
#        print("sshRes:",sshRes)
        ct.sshClose(self.sshClient)
        #等待2s防止文件没有上传成功
        time.sleep(2)
        #匹配文件，并校验文件内容是否有重复的记录
        sshClient = ct.sshConnect(self.hostip, self.port, self.username, self.password)
        #command2 = "ls " + self.remote_dir + self.next_date + "*"
        command2 = "ls '" + self.remote_dir + "FollowSecurity_" + self.next_date + ".csv'"
        logger.info(command2)
        sshRes2 = ct.sshExecCmd(sshClient, command2)
        #print("sshRes2:",sshRes2)
        if len(sshRes2)==1:
            ffilename = sshRes2[0]
            command3 = "cat " + ffilename + " | awk -F\",\" \'{OFS=\",\";print $1}\'"
            #command3 = "cat " + "'" + self.remote_dir + "FollowSecurity_$" + self.next_date + ".csv'" + " | awk -F\",\" \'{OFS=\",\";print $1}\'"
            sshRes3 = ct.sshExecCmd(sshClient, command3)
            #print("sshRes3:",sshRes3)
            if sshRes3 == []:
                msg = "Error: 跟投文件[%s]内容为空" % ffilename
                logger.warning(msg)
                filename = msg
            else:
                df = pd.DataFrame(sshRes3[1:],columns={sshRes3[0]})
                #print("length:", len(df))
                if len(df) != 0 :
                    if len(df[df.duplicated()]) == 0:
                        filename = sshRes2[0]
                    else:
                        dup_df = df[df.duplicated()]
                        dup_list = list(dup_df[sshRes3[0]])
                        dup_str = ','.join(dup_list)
                        msg = "跟投文件[%s]有重复的证券代码记录[%s]" % (ffilename, dup_str)
                        logger.warning(msg)
                        filename = "Error: " + msg
                else:
                    msg = "Error: 跟投文件[%s]内容为空" % ffilename
                    logger.warning(msg)
                    filename = msg
        elif len(sshRes2)==0:
            #temstr = ",".join(sshRes2)
            filename = "Error:没有匹配到跟投文件"
        else:
            filename = "Error:跟投文件匹配多个，" + ",".join(sshRes2)
        ct.sshClose(sshClient)
        return filename
    

    '''
        exchange file检查,取配置文件config/exchange_file_config.txt'来获取服务器信息，
        1,先复制文件到服务器，2，再检查文件是否存在和大小是否正常。
    '''
    def check_exchange_file(self):

        #1，复制windows文件到linux服务器1
        last_workday_i = ct.getLastWorkDay()
        last_workday = last_workday_i.replace('-','')
        file_date_str = last_workday[-4:]
        logger.info("last_workday:" + last_workday)
        winserver = self.follow_hostip
        admin_passwd = self.follow_password
        admin_passwd = "adminadmin\$8"
        sjs_file_dir = "/home/trade/ExchFile/sjs_file/"
        copy_file_name = "SJSGB" + file_date_str + ".DBF"
        new_file_name = "SJSGB" + self.local_date[-4:] + ".DBF"
        #print(new_file_name)
        win_file_local_path = "/D:/tora/back_cmd/sjs_file/" + copy_file_name 
        sjs_back_remote_path = sjs_file_dir + new_file_name
        command = '%sscp_task.sh %s %s %s %s %s' % (sjs_file_dir,winserver,'administrator',admin_passwd,win_file_local_path,
                    sjs_back_remote_path)
        logger.info("cp_file_Command:" + command)
        #本地执行
        #com_res = os.system(command)
        try:
            ret = subprocess.run(command,shell=True,stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,timeout=10,check=False)
            com_res = ret.returncode
            logger.info("com_res:" + str(com_res))
            logger.info("ret.stdout:")
            logger.info(ret.stdout)
            logger.info("ret.stderr:")
            logger.info(ret.stderr)
        except Exception as e:
            msg = "复制文件异常：" + str(e)
            com_res = 256
            logger.error(msg)
        #检查文件是否在
        command_check = "ls " + sjs_back_remote_path
        logger.info("command_check:" + command_check)
        check_res = subprocess.run(command_check,shell=True,stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,timeout=10,check=False)
        logger.info("check_copy_file:")
        logger.info(check_res.stdout)
        #如果执行失败的话，再执行一次。
        if com_res != 0 or check_res.stdout=='':
            logger.info("Failed:从服务器[%s]复制文件[%s]第一次失败" % (winserver,win_file_local_path))
            ret2 = subprocess.run(command,shell=True,stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,timeout=10,check=False)
            com_res2 = ret2.stdout
            logger.info("第二次执行结果com_res2: " + str(com_res2))
        else:
            logger.info("Ok:从服务器[%s]复制文件[%s]成功" % (winserver,win_file_local_path)) 


        #从linux1复制scp到linux2,linux3，要先做scp免密认证，从目的IP到本地7的免密验证。
        #scp trade@192.168.238.7:/home/trade/csvfiles/FollowSecurity_YYYYMMDD.csv /home/trade/run/timaker_hx/follow
        for linux_r_ip in ["10.188.80.16","192.168.253.197","10.188.80.67","192.168.253.135"]:
            #linux_r_ip = '192.168.238.7'
            sshClient_r = ct.sshConnect(linux_r_ip, self.port, self.username, self.password)
            #linux_remote = '/home/trade/ExchFile/sjs_fie/'
            command_linux_r = "scp " + self.username + "@" + self.hostip + ":" + sjs_back_remote_path + " " + sjs_back_remote_path
            logger.info(command_linux_r)
            sshRes = ct.sshExecCmd(sshClient_r, command_linux_r)
    #        print("sshRes:",sshRes)
            #ct.sshClose(sshClient_r)
            #等待2s防止文件没有上传成功
            time.sleep(2)
            #匹配文件，并校验文件内容是否有重复的记录
            #command2 = "ls " + self.remote_dir + self.next_date + "*"
            command2 = "ls " + sjs_back_remote_path
            logger.info(command2)
            sshRes2 = ct.sshExecCmd(sshClient_r, command2)
            ct.sshClose(sshClient_r)

            if len(sshRes2)==0 :
                msg = "服务器[%s],文件[%s]没有匹配到，复制失败！" % (linux_r_ip, new_file_name)
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg)
            else:
                logger.info("服务器[%s],文件[%s]匹配到，复制成功！" % (linux_r_ip, new_file_name))
        
        #2，检查交易所基础文件是否存在，比对文件大小是否为0
        sse_file_dir = "/home/trade/ExchFile/sse/"
        sse_file = ["cpxx0201{mmdd}.txt", "fjy{tradeday}.txt", "gzlx.{Mdd}", "kxx{mmdd}.txt", "xzsl{mmdd}.txt",
                    "dbp{mmdd}.txt", "sfpm01{mmdd}.txt"]
        szse_file_dir = "/home/trade/ExchFile/szse/"
        szse_file = ["securities_{tradeday}.xml", "cashauctionparams_{tradeday}.xml", "issueparams_{tradeday}.xml",
                    "rightsissueparams_{tradeday}.xml","securityswitch_{tradeday}.xml", "imcparams_{tradeday}.xml", 
                    "imcsecurityparams_{tradeday}.xml", "imcexchangerate_{tradeday}.xml","hkexreff04_{tradeday}.txt",
                     "hkexzxjc_{tradeday}.txt"]

        mmdd = self.local_date[-4:]
        tradeday = self.local_date
        Mdd = hex(int(self.local_date[-4:-2]))[-1] + self.local_date[-2:]
        new_sse_file_name=[]
        new_szse_file_name=[]
        check_file_list = [sjs_back_remote_path]

        for file_name in sse_file:
            cc = file_name.replace("{mmdd}",mmdd).replace("{Mdd}",Mdd).replace("{tradeday}",tradeday)
            full_path = sse_file_dir + cc
            new_sse_file_name.append(cc)
            check_file_list.append(full_path)
        print(new_sse_file_name)

        for file_name in szse_file:
            cc = file_name.replace("{tradeday}",tradeday)
            full_path = szse_file_dir + cc
            new_szse_file_name.append(cc)
            check_file_list.append(full_path)
        print(new_szse_file_name)

        #file_list = ["error_log_","download"]
        error_file_list = []
        check_server_list = ["10.188.80.16","192.168.253.197"]
        for check_server_ip in check_server_list:
            self.sshClient = ct.sshConnect(check_server_ip, self.port, self.username, self.password)
            for file_path in check_file_list:
                #file_path = self.remote_dir + file_pre + self.local_date + ".txt"
                try:     
                    file_size = ct.get_remote_filesize(self.sshClient,file_path)     
                    if file_size != '0':
                        logger.info("Ok: 服务器 %s 文件 %s 的大小为 %s ，检查交易所基础数据成功!" % (check_server_ip, file_path, file_size))
                    else:
                        msg = "Error: 盘前交易所基础文件检查失败，服务器 %s 文件 %s 的大小为0或者文件不存在！" % (check_server_ip, file_path)
                        logger.error(msg)
                        error_file_list.append(file_path)
                except Exception as e:
                    msg = "服务器 %s 获取交易所基础文件%s 大小失败，错误信息： %s" % (check_server_ip, file_path, str(e))
                    logger.error(msg)
                    error_file_list.append(file_path)
            ct.sshClose(self.sshClient)
            return error_file_list

        
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
