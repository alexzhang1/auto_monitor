# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 14:13:18 2019

@author: zhangwei
@comment: 检查备份当天文件是否存在，大小是否正常。再检查前一天的数据；  
          正常的话删除2天前的文件，只保留2天的备份文件。周一删除5天前的文件，周二删除4天前的文件。  
"""

import paramiko
import datetime as dt
#import os
import stat
import time
import re
import sys
#import getopt
import logging
import logging.config
#import yaml
import common_tools as ct
#reload(sys)
#sys.setdefaultencoding('utf-8')



logger = logging.getLogger()

class backup_file_check:
    
    def __init__(self, info):
        
            self.hostip = info[0]
            self.port = int(info[1])
            self.username = info[2]
            self.password = info[3]
#            servername = info[4]
            self.remote_dir = info[5]           
            self.hostip_h = info[6]
            self.port_h = int(info[7])
            self.username_h = info[8]
            self.password_h = info[9]
            self.remote_dir_h = info[10]
            self.filetype = info[11]
            self.last_filesize = ''
            #获得当天日期字符串
            self.local_date = dt.datetime.today().strftime('%Y-%m-%d')
            
            #获得上一交易日的日期字符串
            #self.last_WorkDay = self.getLastWorkDay()
            #改成从交易日历获取交易日期
            self.last_WorkDay = ct.get_prevTradeDate(self.local_date)
    # #取得上一工作日的日期（周一--周五）
    # def getLastWorkDay(self, nday=dt.datetime.today()):
        
    #     nday=dt.datetime.today()
    #     #星期几
    #     week = int(time.strftime("%w", nday.timetuple()))
    #     if week == 1:
    #         interval_day = 3
    #     else:
    #         interval_day = 1
    #     lastWorkDay = (dt.datetime.today()-dt.timedelta(interval_day)).strftime('%Y-%m-%d')
    #     return lastWorkDay
    
    # #取得上2个工作日的日期（周一--周五）
    # def getLast2WorkDay(self, nday=dt.datetime.today()):
        
    #     #星期几
    #     week = int(time.strftime("%w", nday.timetuple()))
    #     if week == 1 or week == 2:
    #         interval_day = 4
    #     else:
    #         interval_day = 2
    #     last2WorkDay = (dt.datetime.today()-dt.timedelta(interval_day)).strftime('%Y-%m-%d')
    #     return last2WorkDay


    # ------获取远端linux主机上的文件是否是文件夹
    def get_remote_isdir(self, sftp, path):
        try:
            return stat.S_ISDIR(sftp.stat(path).st_mode)
        except IOError:
            return False


    #递归删除目录,包含自己目录
    def rm_remote_dir(self, sftp, path):
        files = sftp.listdir(path=path)
    
        for f in files:
            filepath = path + '/' + f
            if self.get_remote_isdir(sftp, filepath):
                self.rm_remote_dir(sftp, filepath)
            else:
                #print 'filepath:',  filepath
                sftp.remove(filepath)    
        sftp.rmdir(path)


    # ------获取远端linux主机上指定目录下的所有目录名,返回匹配到名字的文件夹路径------
    def get_match_remote_dir_name(self, sftp, remote_dir, dir_namematch):
        # 保存所有文件的列表
        filedir = ''
        # 去掉路径字符串最后的字符'/'，如果有的话
        if remote_dir[-1] == '/':
            remote_dir = remote_dir[0:-1]
    
        # 获取当前指定目录下的所有目录及文件，包含属性值
        try:
            files = sftp.listdir_attr(remote_dir)
        except Exception as e:
            print("get file_attr exception: ", str(e))
        for x in files:
#            # remote_dir目录中每一个文件或目录的完整路径
#            filename = remote_dir + '/' + x.filename
            # 如果是目录就直记录下来，这里用到了stat库中的S_ISDIR方法，与linux中的宏的名字完全一致
            if stat.S_ISDIR(x.st_mode):
                matchObj = re.match(dir_namematch, x.filename)
                if matchObj:
                    filedir = remote_dir + '/' + x.filename
        return filedir


    #获取远程的文件大小 
    def get_remote_filesize(self, sshClient, filepath):
    
    #    command = 'du -h ' + remote_dir + '/2019-06-12*'
        command = 'du -sh ' + filepath
        logger.info(command)
        stdin,stdout,stderr = sshClient.exec_command(command)
#        stdoutstr = stdout.read()    #python2.7不需要decode
        stdoutstr = stdout.read().decode('utf-8')
        logger.debug(stdoutstr)
#        print("leng:", stderr)
#            stderrstr = stderr.read()      #python2.7不需要decode
        stderrstr = stderr.read().decode('utf-8')
        if stderrstr:
            logger.error(u"exec_command error:" + stderrstr)
        sshRes = []
        sshRes = stdoutstr.strip().split('\n')
#        print "sshRes:", sshRes    
        filesize = sshRes[0].split('\t')[0]
        return filesize

    '''
        检查生成备份文件的服务器(生产机器)，检查当天备份文件目录(匹配local_date + '_16*')
        比较文件夹大小如果>25G则表示成功生成备份文件,检查成功的话会取到上一交易日的备份文件大小并保存下拉供比较使用。
    '''
    def check_origin_file(self):
        try:           
            sshClient = paramiko.SSHClient()
            # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #创建SSH连接
            sshClient.connect(self.hostip, self.port, self.username, self.password)

            # get当天的备份目录大小
            size_check = False
            filematch = self.local_date + '_16*'
            filepath = self.remote_dir + '/' + filematch
            filesize = self.get_remote_filesize(sshClient, filepath)
            logger.info("filesize:" + filesize)
            if filesize:
                logger.debug(u"找到当天的备份目录:")        
                filesizeUnit = filesize[-1]
                filesizeNum = float(filesize[:-1])
                if (filesizeUnit == 'G') and (filesizeNum >= 25):
                    size_check = True
                    logger.info(self.hostip + u"检查备份文件成功！")
                else:
                    msg = "error:服务器: %s 备份文件目录： %s 大小 %s 不正常!" % (self.hostip, filematch, filesize)
                    logger.error(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            else:
                msg = "error: 没有找到服务器： %s 当天的备份目录:%s" % (self.hostip, filematch)
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            #成功后取上一日的备份文件大小，需要和backupserver比较
            if size_check:
                if self.filetype == 'W':
                    last_filematch = self.last_WorkDay  + '_16*'
                    last_filepath = self.remote_dir + '/' + last_filematch
                    la_filesize = self.get_remote_filesize(sshClient, last_filepath)
                    if la_filesize:
                        logger.debug(u"找到上一日的备份目录:" + str(la_filesize))
                        self.last_filesize = la_filesize
                    else:
                        msg = "error: 没有找到服务器： %s 上一日的备份目录:%s" % (self.hostip, last_filematch)
                        logger.error(msg)
                        ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
                else:
                    last_WorkDay_sz = self.last_WorkDay.replace('-','')
                    last_filematch = last_WorkDay_sz + '.tar.gz'
                    last_filepath = self.remote_dir + '/' + last_filematch
                    la_filesize = self.get_remote_filesize(sshClient, last_filepath)
                    if la_filesize:
                        logger.debug(u"找到上一日的备份目录:" + str(la_filesize))
                        self.last_filesize = la_filesize
                    else:
                        msg = "error: 没有找到服务器： %s 上一日的备份目录:%s" % (self.hostip, last_filematch)
                        logger.error(msg)
                        ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            else:
                logger.info(u"当天没有检查成功不检查上日文件")
            sshClient.close()
        except Exception as e:
            logger.error(self.hostip + u" 检查备份文件异常，Exception: " + str(e))
            size_check = False
        finally:
            return size_check

    '''
        检查备份服务器的上一交易日的备份文件大小，和生成备份文件的生产服务器进行比较；大小一样的话比较成功。
    '''
    def check_backupserver_lastday_file(self):            
        try:           
            sshClient = paramiko.SSHClient()
            # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #创建SSH连接
            sshClient.connect(self.hostip_h, self.port_h, self.username_h, self.password_h)
            
            back_check = False
            bs_fsz = ''
            if self.filetype == 'W':
                #宛平为目录
                last_filematch = self.last_WorkDay.replace('-','')
                last_filepath = self.remote_dir_h + '/' + last_filematch
                la_filesize = self.get_remote_filesize(sshClient, last_filepath)
                if la_filesize:
                    logger.debug(u"找到上一日的备份目录:" + str(la_filesize))
                    bs_fsz = la_filesize
                else:
                    msg = "error: 没有找到备份服务器： %s 上一日的备份目录:%s" % (self.hostip_h, last_filematch)
                    logger.error(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            else:
                last_filematch = self.last_WorkDay.replace('-','') + '*'
                last_filepath = self.remote_dir_h + '/' + last_filematch
                la_filesize = self.get_remote_filesize(sshClient, last_filepath)
                if la_filesize:
                    logger.debug(u"找到上一日的备份目录:" + str(la_filesize))
                    bs_fsz = la_filesize
                else:
                    msg = "error: 没有找到备份服务器： %s 上一日的备份目录:%s" % (self.hostip_h, last_filematch)
                    logger.error(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            sshClient.close()
            #比较备份服务器的上一日的备份文件大小 
            if bs_fsz != '' and bs_fsz >= self.last_filesize:
                logger.info(u"ok:备份服务器：%s的文件%s 大小: %s大于等于原始备份文件大小: %s" % (self.hostip_h, last_filepath, bs_fsz, self.last_filesize))
                back_check = True
            else:
                msg = "error:备份服务器：%s的文件%s 大小 %s小于原始服务器 %s 的备份文件大小%s" % (self.hostip_h, last_filepath, bs_fsz, self.hostip, self.last_filesize)
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            
        except Exception as e:
            logger.error(self.hostip + u" 检查备份文件异常，Exception: " + str(e))
            back_check = False
        finally:
            return back_check


    '''
        上一交易日比较备份文件大小成功的话，删除生产服务器2个工作日之前的生成的备份数据文件。filetype == 'W'表示宛平的文件类型，处理方式不一样
    '''
    def del_before_2days_file(self):
        #20200817，直接通过交易日历获取
        #Last2WorkDay = self.getLast2WorkDay()
        prevTradeDate = ct.get_prevTradeDate(self.local_date)
        if prevTradeDate != '0000-00-00':
            Last2WorkDay = ct.get_prevTradeDate(prevTradeDate)
            if self.filetype == 'W':
                #删除目录
                t = paramiko.Transport((self.hostip, self.port))
                t.connect(username=self.username, password=self.password)
                sftp = paramiko.SFTPClient.from_transport(t)
    #            filedirmatch = self.remote_dir + '/' + Last2WorkDay + "_16*"
                dir_namematch = Last2WorkDay + "_16"    
                logger.debug("dir_namematch:" + dir_namematch)
                rmfiledir = self.get_match_remote_dir_name(sftp, self.remote_dir, dir_namematch)
                logger.debug("rmfiledir:" + rmfiledir)
                if rmfiledir == '':
                    msg = "error:没有匹配到服务器%s 两日前的备份目录% s" % (self.hostip, (self.remote_dir + '/' + dir_namematch))
                    logger.error(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
                else:
                    self.rm_remote_dir(sftp, rmfiledir)
                    msg = "ok:删除原始备份服务器%s 备份目录: %s成功！" % (self.hostip, rmfiledir)
                    logger.info(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
                t.close()
            else:
                #删除文件
                t = paramiko.Transport((self.hostip, self.port))
                t.connect(username=self.username, password=self.password)
                sftp = paramiko.SFTPClient.from_transport(t)
                rmfilename = self.remote_dir + '/' + Last2WorkDay.replace('-','') + ".tar.gz"
                logger.debug("rmfile:" + rmfilename)
                try:
                    sftp.remove(rmfilename)
                    msg = "ok:删除原始备份服务器%s 备份文件: %s成功！" % (self.hostip, rmfilename)
                    logger.info(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
                except Exception as e:
                    msg = "error:删除原始备份服务器%s 文件%s 失败,错误原因：%s" % (self.hostip, rmfilename, str(e))
                    logger.error(msg)
                    ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
                t.close()

        else:
            logger.info("今天是非交易日，不删除文件。")

        if self.filetype == 'W':
            #删除目录
            t = paramiko.Transport((self.hostip, self.port))
            t.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(t)
#            filedirmatch = self.remote_dir + '/' + Last2WorkDay + "_16*"
            dir_namematch = Last2WorkDay + "_16"    
            logger.debug("dir_namematch:" + dir_namematch)
            rmfiledir = self.get_match_remote_dir_name(sftp, self.remote_dir, dir_namematch)
            logger.debug("rmfiledir:" + rmfiledir)
            if rmfiledir == '':
                msg = "error:没有匹配到服务器%s 两日前的备份目录% s" % (self.hostip, (self.remote_dir + '/' + dir_namematch))
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            else:
                self.rm_remote_dir(sftp, rmfiledir)
                msg = "ok:删除原始备份服务器%s 备份目录: %s成功！" % (self.hostip, rmfiledir)
                logger.info(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            t.close()
        else:
            #删除文件
            t = paramiko.Transport((self.hostip, self.port))
            t.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(t)
            rmfilename = self.remote_dir + '/' + Last2WorkDay.replace('-','') + ".tar.gz"
            logger.debug("rmfile:" + rmfilename)
            try:
                sftp.remove(rmfilename)
                msg = "ok:删除原始备份服务器%s 备份文件: %s成功！" % (self.hostip, rmfilename)
                logger.info(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            except Exception as e:
                msg = "error:删除原始备份服务器%s 文件%s 失败,错误原因：%s" % (self.hostip, rmfilename, str(e))
                logger.error(msg)
                ct.send_sms_control('NoLimit',msg,'13162583883,13681919346')
            t.close()

def main(argv):
  
    try:
        yaml_path = './config/backup_file_check_logger.yaml'
        ct.setup_logging(yaml_path)
        linuxInfo = ct.get_server_config('./config/backup_file_check_config.txt')
        
        for info in linuxInfo:               
            
            bfc = backup_file_check(info)
            origin_check_flag = bfc.check_origin_file()
            back_check = False
            if origin_check_flag:
                back_check = bfc.check_backupserver_lastday_file()
            if back_check:
                bfc.del_before_2days_file()

    except Exception:
        logger.error('Faild to check backup file!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)




if __name__ == '__main__':
#    print('start:%s' %time.ctime())
    main(sys.argv[1:])
#    print('end:%s' %time.ctime())
