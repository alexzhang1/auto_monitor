# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:01:34 2019

@author: zhangwei
@comment: 监控类方法，监控远程linux服务器的硬盘，内存，端口，进程，监控fpga文件大小,ping等监控任务。
"""
import paramiko
import re
import os
import time
import datetime as dt
import common_tools as ct
import socket
import logging
import platform
import subprocess



logger = logging.getLogger('main.monitor_server_status')
#ntime = dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") 
ndate = dt.datetime.now().strftime("%Y-%m-%d")
cur_dir_i=os.getcwd()
cur_dir = cur_dir_i.replace("\\","/") + "/"

if os.path.exists("monitor_result"):
    pass
else:
    os.mkdir('monitor_result')
result_file = cur_dir + "monitor_result/" + "status_result_" + ndate + '.txt'
#log_file = cur_dir + "mylog/" + "monitor_log_" + ndate + '.txt'
error_log_file = cur_dir + "mylog/" + "status_error_log_" + ndate + '.txt'


class MonitorServer:
    
    def __init__(self, linuxInfo, single_handle=None):
        
        self.linuxInfo = linuxInfo
        self.single_info = single_handle
        self.sshClient = None        
        self.mem_info_verify = False
        self.disk_info_verify = False
        self.netstat_info_verify = False
        self.ps_info_verify = False
        self.socket_info_verify = False
        self.fpga_file_info_verify = False
        self.ping_info_verify = False
        self.single_info_verify = False
        self.Check_flag_list = []
        self.fpga_Check_flag_list = []
        self.SocPs_Check_flag_list = []
        self.mem_Check_flag_list = []
        self.ping_Check_flag_list = []
        self.disk_Check_flag_list = []
        self.common_Check_flag_list = []
        

    
    def get_all_Check_flag(self):
        return (self.mem_info_verify and self.disk_info_verify and self.ps_info_verify and self.socket_info_verify)


    '''
    创建 ssh 连接函数
    hostip, port, username, password,访问linux的ip，端口，用户名以及密码
    '''
    def sshConnect(self, hostip, port, username, password):
        paramiko.util.log_to_file('./mylog/paramiko.log')
        try:
            #创建一个SSH客户端client对象
            sshClient = paramiko.SSHClient()
            # 获取客户端host_keys,默认~/.ssh/known_hosts,非默认路径需指定
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #创建SSH连接
            sshClient.connect(hostip, port, username, password)
#            ct.write_log(log_file,"SSH connect success!")
            logger.debug("SSH connect success!")
        except Exception as e:
#            msg = "SSH connect failed: [hostip:%s];[username:%s];[error:%s]" %(hostip,username,e)
#            print msg
#            ct.write_log(error_log_file,msg)
            msg = "SSH connect failed: [hostip:%s];[username:%s];[error:%s]" % (hostip, username, str(e))
            logger.error(msg)
            ct.send_sms_control('NoLimit',msg)
#            exit()
        return sshClient
    '''
    创建命令执行函数
    command 传入linux运行指令
    '''
    def sshExecCmd(self,command):

        stdin, stdout, stderr = self.sshClient.exec_command(command)
        if stderr:
            stderrstr = stderr.read()
            #print("err:", stderrstr.decode('utf-8'))
            logger.warning(u"exec_command error:" + stderrstr.decode('utf-8'))
#        print ("stdout: ", stdout)
#        print "type: ", type(stdout)
#        results=stdout.read()
#        print "results:", results
#        print(stdout.read().decode('utf8'))
        filesystem_usage = stdout.readlines()
        return filesystem_usage
    
    '''
    关闭ssh
    '''
    def sshClose(self):
        self.sshClient.close()


#    '''
#    执行远程命令获取执行结果
#    '''
#    def get_remote_execute_res(self, info):
#             
#        hostip = info[0]
##        port = info[1]
##        username = info[2]
##        password = info[3]
##        servername = info[4]
#        command = info[5]
#        
#        self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
#        sshRes = self.sshExecCmd(self.sshClient, command)
#        logger.info(hostip + "::" + command)
#
#        for item in sshRes:
#            temstr= item.strip()
#            logger.info(temstr)
#            
#        self.sshClose()
#        return sshRes
#        print("get_query_data finished")    


    '''
    fpga文件监控
    '''
    def fpga_monitor_run(self):

        logger.info("The fpga monitor Starting... ")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.fpga_file_info(info)
            
            self.sshClose()

        logger.info("The fpga_monitor_run Stoped")

    '''
    常规监控
    '''
    def monitor_run(self):

        logger.info("The monitor Starting...")
        for info in self.linuxInfo:
#            hostip = info[0]
#            port = info[1]
#            username = info[2]
#            password = info[3]
#            servername = info[4]
#            processes = info[5]
#            ports_i=info[6]
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.disk_info(info)
            self.mem_info(info)
            self.socket_info(info)
            self.ps_info(info)
            
            self.sshClose()
            all_Check_flag = self.get_all_Check_flag()
            if all_Check_flag:
                self.Check_flag_list.append(1)
            else:
                self.Check_flag_list.append(0)
        logger.info("The monitor Stoped")
        

    #检查端口和进程是否正常
    def socket_ps_monitor(self):

        logger.info("The socket_ps monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.socket_info(info)
            self.ps_info(info)
            
            self.sshClose()
            SocPs_Check_flag = self.ps_info_verify and self.socket_info_verify
            if SocPs_Check_flag:
                self.SocPs_Check_flag_list.append(1)
            else:
                self.SocPs_Check_flag_list.append(0)
        logger.info("The socket_ps monitor Stoped")


    #非交易时间检查进程是否正常关闭
    def non_trade_ps_monitor(self):

        logger.info("The non trade ps monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.non_trade_ps_info(info)
            
            self.sshClose()
            SocPs_Check_flag = self.ps_info_verify
            if SocPs_Check_flag:
                self.SocPs_Check_flag_list.append(1)
            else:
                self.SocPs_Check_flag_list.append(0)
        logger.info("The non trade ps monitor Stoped")


    #检查磁盘存贮是否正常
    def disk_monitor(self):

        logger.info("The disk monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.disk_info(info)
            
            self.sshClose()
            disk_Check_flag = self.disk_info_verify
            if disk_Check_flag:
                self.disk_Check_flag_list.append(1)
            else:
                self.disk_Check_flag_list.append(0)
        logger.info("The disk monitor Stoped")


    #检查内存是否正常
    def mem_monitor(self):

        logger.info("The mem monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3])
            
            self.mem_info(info)
            
            self.sshClose()
            mem_Check_flag = self.mem_info_verify
            if mem_Check_flag:
                self.mem_Check_flag_list.append(1)
            else:
                self.mem_Check_flag_list.append(0)
        logger.info("The mem monitor Stoped")
        


    #盘后检查内存是否正常并处理
    def non_trade_mem_monitor(self):
#            hostip = info[0]
#            port = info[1]
        username = 'root'
        password = 'root123'
#            servername = info[4]
        logger.info("The mem monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], username, password)
            
            self.non_trade_mem_info(info)
            
            self.sshClose()
            mem_Check_flag = self.mem_info_verify
            if mem_Check_flag:
                self.mem_Check_flag_list.append(1)
            else:
                self.mem_Check_flag_list.append(0)
        logger.info("The mem monitor Stoped")
        
        
     #ping服务器是否正常
    def ping_server_monitor(self):

        logger.info("The Ping monitor Starting...")
        for info in self.linuxInfo:
                       
            self.ping_server_info(info)
            ping_Check_flag = self.ping_info_verify
            if ping_Check_flag:
                self.ping_Check_flag_list.append(1)
            else:
                self.ping_Check_flag_list.append(0)
        logger.info("The Ping monitor Stoped")       
        
            

     #检查core文件
    def core_file_monitor(self):

        logger.info("The core file monitor Starting...")
        for info in self.linuxInfo:
            
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3]) 
            
            self.core_file_info(info)
            single_Check_flag = self.single_info_verify
            if single_Check_flag:
                self.common_Check_flag_list.append(1)
            else:
                self.common_Check_flag_list.append(0)
            
            self.sshClose()
        logger.info("The core file monitor Stoped")   



     #单个的监控任务,single_info为单项监控信息处理函数
    def single_common_monitor(self):

        logger.info("The %s monitor Starting..." % self.single_info)
        for info in self.linuxInfo:

            execute_func = getattr(self,self.single_info)          
            self.sshClient = self.sshConnect(info[0], info[1], info[2], info[3]) 
            execute_func(info)
#            self.core_file_info(info)
            single_Check_flag = self.single_info_verify
            if single_Check_flag:
                self.common_Check_flag_list.append(1)
            else:
                self.common_Check_flag_list.append(0)         
            self.sshClose()
        logger.info("The %s monitor Stoped" % self.single_info)   



    '''单一台服务器的每个监控信息的执行 '''  
    '''
    Ping监控
    '''
    def ping_server_info(self, info):

        hostip = info[0]
        
        sysstr = platform.system()
        if sysstr == "Windows":
            logger.debug('ping ' + hostip)
            ping = subprocess.Popen('ping ' + hostip,
                                    shell=True,
                                    stderr=subprocess.PIPE,
                                    stdout=subprocess.PIPE) # 执行命令
            res,err = ping.communicate()
#            print("err:", err.decode('gbk'))
#            if err: sys.exit(err.decode('gbk').strip('\n'))
            if err:
                logger.warning("ping error: %s" % str(err))
                pres = []
            else:
                pres = list(res.decode('gbk').split('\n'))
                logger.debug("pres:",pres)
            try:
                loss = pres[8].split('(')[1].split('%')[0] + "%"  # 获取丢包率
            except IndexError:
                loss = "100%"       
            try:
                rtt = pres[10].split('=')[3].split('ms')[0] # 获取rtt avg值
            except IndexError:
                rtt = ""
        else:
    #        ping = subprocess.Popen('ping -i 0.2 -c 4 -q -I ' + src + ' ' + dest,
            #-I<网络界面> 使用指定的网络接口送出数据包
            ping = subprocess.Popen('ping -i 1 -c 4 -q ' + hostip,
                                    shell=True,
                                    stderr=subprocess.PIPE,
                                    stdout=subprocess.PIPE) # 执行命令
            res,err = ping.communicate()
#            print("err:", err.decode('gbk'))
#            if err: sys.exit(err.decode('gbk').strip('\n'))
            if err:
                logger.warning("ping error: %s" % str(err))
                pres = []
            else:
                pres = list(res.decode('gbk').split('\n'))
                logger.debug("pres:",pres)
            try:
                #tem = "4 packets transmitted, 0 received, 100% packet loss, time 611ms"
                loss = pres[3].split()[5]  # 获取丢包率
                #loss = tem.split()[5]
            except IndexError:
                loss = "100%"
            try:
                rtt = pres[4].split('/')[4] # 获取rtt avg值            
            except IndexError:
                rtt = "9999"
        # loss>0,rtt>800报警
        if float(loss.strip('%')) > 0 or float(rtt) > 800 :
            self.ping_info_verify = False
            msg = "error:" + hostip + " ::The ping lost is " + loss + " rtt is " + rtt + "ms"
            ct.write_log(error_log_file,msg)
            logger.error(msg)
            ct.send_sms_control("ping", msg)
        else:
            self.ping_info_verify = True
            msg = "ok:" + hostip + " ::The ping lost is " + loss + " rtt is " + rtt + "ms"
            logger.info(msg)
        msg = "Ping Check Result: " + str(self.ping_info_verify)
        logger.info(msg)



    '''内存监控'''
    def mem_info(self, info):
        command = 'cat /proc/meminfo'
        hostip = info[0]
#        servername = info[4]
#        #非交易时间自动清理一下缓存
#        start_time = '08:45'
#        end_time = '15:30'
#        if not (ct.time_check(start_time, end_time)):
#            self.mem_info_verify = True
#            logger.info("Clear BuffersCachedRate")
#        else:
#            logger.info("Not to clear BuffersCachedRate")
        sshRes = self.sshExecCmd(command)
        mem_values = re.findall("(\d+)\ kB", ",".join(sshRes))
        MemTotal = mem_values[0]
        MemFree = mem_values[1]
        MemAvailable = mem_values[2]
        Buffers = mem_values[3]
        Cached = mem_values[4]
        SwapCached = mem_values[5]
        SwapTotal = mem_values[14]
        SwapFree = mem_values[15]
        logger.info('******************************Mem Monitor: [server:%s]*********************************' % hostip)
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        ct.write_file(result_file, cur_time + "::" + hostip + "_mem_info_result:")
        titlename="MemTotal,MemFree,MemAvailable,Buffers,Cached,SwapCached,SwapTotal,SwapFree,BuffersCachedRate,Rate_Mem"
        logger.debug(titlename)
        ct.write_file(result_file, titlename)
        #计算b/cRate，RateMem
        BuffersCachedRate = round(100 * (int(Buffers) + int(Cached)) / float(MemTotal), 2)
        logger.debug("BuffersCachedRate:" + str("%.2f" % BuffersCachedRate) + "%")
        Free_Mem = int(MemFree) + int(Buffers) + int(Cached)
        Used_Mem = int(MemTotal) - Free_Mem
        Rate_Mem = round(100 * Used_Mem / float(MemTotal),2)
        logger.debug("Rate_Mem:" + str("%.2f" % Rate_Mem) + "%")
        tem_list = [MemTotal,MemFree,MemAvailable,Buffers,Cached,SwapCached,SwapTotal,SwapFree,BuffersCachedRate,Rate_Mem]
        temp = map(str,tem_list)
        memstr=','.join(temp)
        logger.debug(memstr)
        ct.write_file(result_file, memstr)
        # BuffersCachedRate > 50报警,交易时间不判断
        start_time = '08:45'
        end_time = '15:30'
        if (ct.time_check(start_time, end_time)):
            self.mem_info_verify = True
            logger.info("Not to check BuffersCachedRate")
        else:
            if BuffersCachedRate < 50:
                self.mem_info_verify = True
                msg = "ok:" + hostip + " ::The BuffersCachedRate is " + str(BuffersCachedRate) + " % is ok"   
                logger.info(msg)
            else:
                self.mem_info_verify = False
                msg = "error:" + hostip + " ::The BuffersCachedRate is " + str(BuffersCachedRate) + " % is overload"
                ct.write_log(error_log_file,msg)
                logger.error(msg)
                ct.send_sms_control("mem", msg)
        # Rate_Mem>80报警
        if Rate_Mem < 80:
            self.mem_info_verify = self.mem_info_verify and True
            msg = "ok:" + hostip + " ::The Rate_Mem is " + str(Rate_Mem) + " % is ok"
            logger.info(msg)
        else:
            self.mem_info_verify = False
            msg = "error:" + hostip + " ::The Rate_Mem is " + str(Rate_Mem) + " % is overload"
            ct.write_log(error_log_file,msg)
            logger.error(msg)
            ct.send_sms_control("mem", msg)
        msg = "Mem Check Result: " + str(self.mem_info_verify)
        logger.info(msg)


    """ 
    磁盘空间监控
    """

    def disk_info(self,info):
        command = 'df -h'
        hostip = info[0]
#        servername = info[4]
        sshRes = self.sshExecCmd(command)
#        print "sshRes:", sshRes
        sshResStr = ''.join(sshRes)
        sshResList = sshResStr.strip().split('\n')
        df_info_list=[]
        for disk in sshResList[1:]:
            df_info_list.append(disk.strip().split())
#        print "df_info_list:", df_info_list
#        print "len(df_info_list):", len(df_info_list)
        
        sshResLists=[]
        for i in range(len(df_info_list)):
            if len(df_info_list[i]) == 1 and len(df_info_list[i+1]) == 5:
                sshResLists.append(df_info_list[i]+df_info_list[i+1])
            elif len(df_info_list[i]) == 6:
                sshResLists.append(df_info_list[i])
            elif len(df_info_list[i])!=1 and len(df_info_list[i])!=5:
                msg = "The df_info's format is not correct!"
#                print msg
                ct.write_file(error_log_file, msg)
                logger.error(msg)
#        print "sshResLists:",sshResLists
#        print "len(sshResLists):", len(sshResLists)

        logger.info("************************Disk Monitor: [server:%s]****************************"% hostip)
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        ct.write_file(result_file,cur_time + "::" + hostip + "_disk_info_result:")
        titlename = "Filesystem,Size,Used,Avail,Use%,Mounted on"
        logger.debug(titlename)
        ct.write_file(result_file, titlename)
        if len(sshResLists):
            self.disk_info_verify = True
        for disklist in sshResLists:
            diskstr=','.join(disklist)
            logger.debug(diskstr)
            ct.write_file(result_file, diskstr)
            Use_Rate = int(disklist[4].split('%')[0])
            #匹配网络路径
            matchObj = re.search( r'//.*?/', disklist[0], re.M|re.I)            
            #磁盘空间已用%>80报警，去掉mnt/cdrom和//ip/path这样的文件
            #if (disklist[0]!="/dev/sr0") and not(matchObj):   
            if (disklist[0] not in ["/dev/sr0","/dev/sr1"]) and not(matchObj):                  
                if Use_Rate < 80:
                    self.disk_info_verify = self.disk_info_verify and True
                    msg = "ok:" + hostip + "::" + disklist[0] + " ::The Use% is " + str(Use_Rate) + " % is ok"
                    logger.info(msg)
                else:
                    self.disk_info_verify = False
                    msg = "error:" + hostip + "::" + disklist[0] + " ::The Use% is " + str(Use_Rate) + " % is overload"
#                    print msg
                    ct.write_log(error_log_file,msg)
                    logger.error(msg)
                    ct.send_sms_control("disk", msg)
        msg = "Disk Check Result: " + str(self.disk_info_verify)
#        print msg
#        ct.write_log(log_file,msg)
        logger.info(msg)

        
    """ 
    ps进程监控
    """
    def ps_info(self,info):
        
        hostip = info[0]
        username = info[2]
#        servername = info[4]
        processes = info[5]
        process_count = len(str(processes).split('|'))
        command = 'ps -u ' + username + ' -elf | grep -E "' + processes + '" | grep -v grep'
#        command = 'ps -u trade -elf |grep -E "dbsync 1|dbsync 2" | grep -v grep'
        logger.debug("command: " + command)
        sshRes = self.sshExecCmd(command)
#        print "sshRes:", sshRes
        if sshRes == []:
            self.ps_info_verify = False
            msg = "error: Server %s The count of the processes is 0, please check it" % str(hostip)
            ct.write_log(error_log_file, msg)
            logger.error(msg)
            ct.send_sms_control("ps_port", msg)
        else:
            sshResStr = ''.join(sshRes)
#            print("sshResStr: ", sshResStr)
            sshResList = sshResStr.strip().split('\n')
    #        print "sshResList: ", sshResList
            sshResLists = []
            for sshCom in sshResList:
                sshResLists.append(sshCom.strip().split())
            logger.info("******************************Processes Monitor: [server:%s]*********************************"% hostip)
            cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            ct.write_file(result_file,cur_time + "::" + hostip + "_ps_info_result:")
    #        print "sshResLists:\n", sshResLists
    #                F S   UID    PID   PPID  C PRI  NI ADDR SZ WCHAN  TTY          TIME CMD
            titlename="F,S,UID,PID,PPID,C,PRI,NI,ADDR,SZ,WCHAN,TTY,TIME,CMD"
            logger.debug(titlename)
            ct.write_file(result_file,titlename)
            #判断线程数量是否正确
            self.ps_info_verify = (process_count == len(sshRes))
            if (self.ps_info_verify):
                msg = "ok: The query count of the processes is " + str(len(sshRes))
                logger.info(msg)
                #再检查进程的状态是否正确
                for datalist in sshResLists:
        
                    self.ps_info_verify = True
                    psstr=','.join(datalist)
                    logger.debug(psstr)
    #                chg_psstr = psstr.encode('utf-8')
                    ct.write_file(result_file, psstr)
                    if datalist[1] in ['R','S','D']:
                        self.ps_info_verify = self.ps_info_verify and True
                        msg = "ok:" + hostip + ":: Time:" + str(datalist[13]) + " ::The state is " + str(datalist[1]) + " is ok"
                        logger.info(msg)
                    else:
                        self.ps_info_verify = False
                        msg = "error:" + hostip + ":: Time: " + str(datalist[13]) + " ::The state is " + str(datalist[1]) + " is not correct"
                        ct.write_log(error_log_file, msg)
                        logger.error(msg)
                        ct.send_sms_control("ps_port", msg)
               
            else:
                msg = "error: Server %s The query count %s of the processes is not equal: %s" % (hostip, str(len(sshRes)), str(process_count))
                ct.write_log(error_log_file, msg)
                logger.error(msg)
                ct.send_sms_control("ps_port", msg)
            

        msg = "ps Processes Check Result: " + str(self.ps_info_verify)
        logger.info(msg)



    """ 
    非交易时间ps进程监控

    """
    def non_trade_ps_info(self,info):
        
        hostip = info[0]
        username = info[2]
#        servername = info[4]
        processes = info[5]
#        process_count = len(str(processes).split('|'))
        command = 'ps -u ' + username + ' -elf | grep -E "' + processes + '" | grep -v grep'
#        command = 'ps -u trade -elf |grep -E "dbsync 1|dbsync 2" | grep -v grep'
        logger.info("command: " + command)
        sshRes = self.sshExecCmd(command)
#        print "sshRes:", sshRes
        if sshRes == []:
            self.ps_info_verify = True
            msg = "OK: Server %s The count of the processes is 0 " % str(hostip)
            logger.info(msg)
        else:
            self.ps_info_verify = False
            sshResStr = ''.join(sshRes)
            sshResList = sshResStr.strip().split('\n')
    #        print "sshResList: ", sshResList
            sshResLists = []
            for sshCom in sshResList:
                sshResLists.append(sshCom.strip().split())
#    #        print "sshResLists:\n", sshResLists
#            titlename="F,S,UID,PID,PPID,C,PRI,NI,ADDR,SZ,WCHAN,TTY,TIME,CMD"

            ps_list = []
            for datalist in sshResLists:    
#                psstr=','.join(datalist)
                psstr = ' '.join(datalist[14:])
                logger.info("ps:" + psstr)
#                chg_psstr = psstr.encode('utf-8')
                ct.write_log(error_log_file, psstr)
                msg = "error:" + hostip + " ::The process is " + psstr + ":: Time: " + str(datalist[13]) + " is still working!"
                ct.write_log(error_log_file, msg)
                logger.warning(msg)
                ps_list.append(psstr)
            ps_cmd =';'.join(ps_list)
            sms_msg = "error:" + hostip + " ::Processes : " + ps_cmd + " is still working!"
            ct.send_sms_control("ps_port", sms_msg)

        msg = "ps Processes Check Result: " + str(self.ps_info_verify)
        logger.info(msg)


    '''
    使用socket验证端口是否打开
    '''
    def socket_info(self,info):
        
        hostip = info[0]
#        servername = info[4]
        ports_i=info[6]
        ports = ports_i.split(';')
#        print "port:", ports
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        logger.info("******************************Ports Monitor: [server:%s]*********************************"% hostip)
        ct.write_file(result_file,cur_time + "::" + hostip + "_socket_port_info_result:")
        flag_list=[]
        for port in ports:
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.settimeout(3)
            try:
                sk.connect((hostip, int(port)))
                msg = "ok:" + str(hostip) + ":" + str(port) + " is ok"
                ct.write_file(result_file, msg)
                logger.info(msg)
                list_flag = 1
                flag_list.append(list_flag)
            except Exception:
        #        print "\033[1;31;mServer port 18000 is close\033[0m"
                msg = "error: " + str(hostip) + ":" + str(port) + " is closed"
                logger.error(msg)
                ct.send_sms_control("ps_port", msg)
                ct.write_file(error_log_file, msg)
                ct.write_file(result_file, msg)
                list_flag = 0
                flag_list.append(list_flag)
            sk.close()
#        print "flag_list: ", flag_list
        if len(flag_list) == 0:
            self.socket_info_verify = False
        else:
            self.socket_info_verify = (sum(flag_list)==len(flag_list))
        msg = "socket Ports Check Result: " + hostip + "::" + str(self.socket_info_verify)
        logger.info(msg)
        
    '''
    检查FPGA文件目录
    '''
    def fpga_file_info(self,info):
#        info = ['192.168.238.7', 22, 'trade', 'trade', 'FPGAServer','/home/trade/FPGA']
        hostip = info[0]
#        servername = info[4]
        filepath = info[5]
        command = 'ls -l ' + filepath
        logger.info(command)
        sshRes = self.sshExecCmd(command)
#        print "sshRes:", sshRes
        if sshRes == []:
            self.fpga_file_info_verify = False
            msg = "error: The sshResturn is None, please check it"
#            print msg
            ct.write_log(error_log_file, msg)
            logger.warning(msg)
        else:
            sshResStr = ''.join(sshRes)
#            print "sshResStr: ", sshResStr
            sshResList = sshResStr.strip().split('\n')
    #        print "sshResList: ", sshResList
            sshResLists = []
            for sshCom in sshResList:
                sshResLists.append(sshCom.strip().split())
#            print "len(sshResLists):", len(sshResLists)
            logger.info("******************************FPGA Monitor: [server:%s]*********************************"% hostip)
            cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            ct.write_file(result_file,cur_time + "::" + hostip + "_ps_info_result:")
#            print("sshResLists:\n", sshResLists)
            titlename="TypePermission,ConnectedCount,Owner,Group,Size,ModifyMonth,ModifyDate,ModifyTime,FName"
            logger.debug(titlename)
            ct.write_file(result_file,titlename)
            #从第二行开始
            fileSize_dict = {'sent0':None,'received0':None,'received1':None}
            for datalist in sshResLists[1:]:
                lsstr=','.join(datalist)
                logger.debug(lsstr)
#                chg_str = lsstr.encode('utf-8')
#                print("aaggg:", chg_str)
                ct.write_file(result_file, lsstr)                               
                if len(datalist)==9:
                    Size = int(datalist[4])
                    FName = datalist[8]
                    Today = dt.datetime.now().strftime("%Y%m%d")
#                    matchlist=['sent','journal','received']
                    str1 = 'FPGA0_CSESSION00_' + Today + '.sent'
                    str2 = 'FPGA0_VSESSION00_' + Today + '.received'
                    str3 = 'FPGA0_VSESSION01_' + Today + '.received'
                    
                    if str1 == FName:
                        fileSize_dict['sent0'] = Size
#                        print("Fname:",FName,Size)                        
                    if str2 == FName:
                        fileSize_dict['received0'] = Size
#                        print("Fname:",FName,Size)
                    if str3 == FName:
                        fileSize_dict['received1'] = Size
#                        print("Fname:",FName,Size)    
            logger.info(fileSize_dict)
            ntime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            dictstr = ntime + "::FPGA file size: " + str(fileSize_dict)
            ct.write_file(result_file, dictstr)
#            self.fpga_file_info_verify = (fileSize_dict['sent0'] or fileSize_dict['sent1']) and (fileSize_dict['journal0'] or fileSize_dict['journal1']) and (fileSize_dict['received0'] or fileSize_dict['received1'])
            self.fpga_file_info_verify = (fileSize_dict['sent0'] and (fileSize_dict['received0'] and fileSize_dict['received1']))
            if self.fpga_file_info_verify:
                msg = "ok: The server %s FPGA Monitor is ok, %s " % (hostip, dictstr)
                logger.info(msg)
                self.fpga_Check_flag_list.append(1)
            else:
                msg = "error: The server %s FPGA Monitor is not correct, %s " % (hostip, dictstr)
                ct.write_log(error_log_file, msg)
                logger.error(msg)
                ct.send_sms_control("fpga", msg)
                self.fpga_Check_flag_list.append(0)
                

    '''盘后内存监控及处理'''
    def non_trade_mem_info(self, info):
        command = 'cat /proc/meminfo'
        hostip = info[0]
#        servername = info[4]
#        #非交易时间自动清理一下缓存
#        start_time = '08:45'
#        end_time = '15:30'
#        if not (ct.time_check(start_time, end_time)):
#            self.mem_info_verify = True
#            logger.info("Clear BuffersCachedRate")
#        else:
#            logger.info("Not to clear BuffersCachedRate")
        sshRes = self.sshExecCmd(command)
        mem_values = re.findall("(\d+)\ kB", ",".join(sshRes))
        MemTotal = mem_values[0]
        MemFree = mem_values[1]
        MemAvailable = mem_values[2]
        Buffers = mem_values[3]
        Cached = mem_values[4]
        SwapCached = mem_values[5]
        SwapTotal = mem_values[14]
        SwapFree = mem_values[15]
        logger.info('******************************Mem Monitor: [server:%s]*********************************' % hostip)
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        ct.write_file(result_file, cur_time + "::" + hostip + "_mem_info_result:")
        titlename="MemTotal,MemFree,MemAvailable,Buffers,Cached,SwapCached,SwapTotal,SwapFree,BuffersCachedRate,Rate_Mem"
        logger.info(titlename)
        ct.write_file(result_file, titlename)
        #计算b/cRate，RateMem
        BuffersCachedRate = round(100 * (int(Buffers) + int(Cached)) / float(MemTotal), 2)
        logger.info("BuffersCachedRate:" + str("%.2f" % BuffersCachedRate) + "%")
        Free_Mem = int(MemFree) + int(Buffers) + int(Cached)
        Used_Mem = int(MemTotal) - Free_Mem
        Rate_Mem = round(100 * Used_Mem / float(MemTotal),2)
        logger.info("Rate_Mem:" + str("%.2f" % Rate_Mem) + "%")
        tem_list = [MemTotal,MemFree,MemAvailable,Buffers,Cached,SwapCached,SwapTotal,SwapFree,BuffersCachedRate,Rate_Mem]
        temp = map(str,tem_list)
        memstr=','.join(temp)
        logger.debug(memstr)
        ct.write_file(result_file, memstr)
        # BuffersCachedRate > 50报警,交易时间不判断
        start_time = '08:45'
        end_time = '15:30'
        if (ct.time_check(start_time, end_time)):
            self.mem_info_verify = True
            logger.info("Not to check BuffersCachedRate")
        else:
            if BuffersCachedRate < 50:
                self.mem_info_verify = True
                msg = "ok:" + hostip + " ::The BuffersCachedRate is " + str(BuffersCachedRate) + " % is ok"    
                logger.info(msg)
            else:
                #清理缓存
                command_clear = 'sync;echo 3 > /proc/sys/vm/drop_caches'
                sshRes_clear = self.sshExecCmd(command_clear)
                logger.debug(sshRes_clear)
                #再次检查一次
                sshRes = self.sshExecCmd(command)
                mem_values = re.findall("(\d+)\ kB", ",".join(sshRes))
                MemTotal = mem_values[0]
                MemFree = mem_values[1]
                MemAvailable = mem_values[2]
                Buffers = mem_values[3]
                Cached = mem_values[4]
                SwapCached = mem_values[5]
                SwapTotal = mem_values[14]
                SwapFree = mem_values[15]
                logger.info('******************************Mem Monitor2: [server:%s]*********************************' % hostip)
                cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                ct.write_file(result_file, cur_time + "::" + hostip + "_mem_info_result:")
                titlename="MemTotal,MemFree,MemAvailable,Buffers,Cached,SwapCached,SwapTotal,SwapFree,BuffersCachedRate,Rate_Mem"
                logger.info(titlename)
                ct.write_file(result_file, titlename)
                #计算b/cRate，RateMem
                BuffersCachedRate = round(100 * (int(Buffers) + int(Cached)) / float(MemTotal), 2)
                logger.info("BuffersCachedRate:" + str("%.2f" % BuffersCachedRate) + "%")
                #如果还大于等于50，则报警
                if BuffersCachedRate >= 50:
                    self.mem_info_verify = False
                    msg = "error:" + hostip + " ::The BuffersCachedRate is " + str(BuffersCachedRate) + " % is overload"
                    ct.write_log(error_log_file,msg)
                    logger.error(msg)
                    ct.send_sms_control("mem", msg)
        # Rate_Mem>80报警
        if Rate_Mem < 80:
            self.mem_info_verify = self.mem_info_verify and True
            msg = "ok:" + hostip + " ::The Rate_Mem is " + str(Rate_Mem) + " % is ok"
            logger.info(msg)
        else:
            self.mem_info_verify = False
            msg = "error:" + hostip + " ::The Rate_Mem is " + str(Rate_Mem) + " % is overload"
            ct.write_log(error_log_file,msg)
            logger.error(msg)
            ct.send_sms_control("mem", msg)
        msg = "Mem Check Result: " + str(self.mem_info_verify)
        logger.info(msg)
        
        

    """ 
    core文件监控
    """
    def core_file_info(self,info):
        command = 'find /home/trade/temp -name core.*'
        hostip = info[0]
#        servername = info[4]
        logger.info("command: " + command)
        sshRes = self.sshExecCmd(command)
#        print("sshRes:", sshRes)
        if sshRes == []:
            self.single_info_verify = True
            msg = "OK: Server %s The count of core file is 0 " % str(hostip)
            logger.info(msg)
        else:
            self.single_info_verify = False
            sshResStr = ''.join(sshRes)
            sshResList = sshResStr.strip().split('\n')
            print("sshResList: ", sshResList)
#            ps_list = []
            for datalist in sshResList:    

                msg = "error: " + hostip + " Have core file:" + datalist 
                ct.write_log(error_log_file, msg)
                logger.warning(msg)
            
            sms_msg = "error: " + hostip + " 有core文件，请检查服务器文件"
            logger.error(sms_msg)
            ct.send_sms_control("core", sms_msg)

        msg = "core file Check Result: " + str(self.single_info_verify)
        logger.info(msg)



    """ 
    tcp连接监控
    """
    def tcp_connect_info(self,info):
        tcp_ports = info[6].split(';')
        hostip = info[0]
        for tcp_port in tcp_ports:
            command = '/sbin/ss -anp | grep ' + tcp_port
            #command = 'df -h'
            
    #        servername = info[4]
            logger.info("command: " + command)
            sshRes = self.sshExecCmd(command)
            # print("sshRes:", sshRes)
            if sshRes == []:
                #self.single_info_verify = True
                msg = "Server[%s] port [%s] tcp connect count is 0 " % (str(hostip), tcp_port)
                logger.info(msg)
            else:
                total_count = len(sshRes)
                total_limit = 500
                single_user_limit = 100
                #总连接数大于等于500报警。
                if total_count >= total_limit:
                    #self.single_info_verify = False
                    total_count_verify = False
                    msg = "Server[%s] port [%s] tcp connect total count[%d] is out of [%d] "\
                         % (str(hostip), tcp_port, total_count, total_limit)
                    logger.info(msg)
                    ct.send_sms_control('NoLimit', msg)
                else:
                    total_count_verify = True
                    msg = "Server[%s] port [%s] tcp connect total count[%d]."\
                         % (str(hostip), tcp_port, total_count)
                    logger.info(msg)
                
                #检查单个IP的连接数
                self.single_info_verify = False
                sshResStr = ''.join(sshRes)
                sshResList = sshResStr.strip().split('\n')
                # print("sshResList: ", sshResList)
                tcp_con_list = []
                client_ip_list = []

                for datalist in sshResList:
                    # print("datalist:", datalist)
                    # print("length and type", len(datalist), type(datalist))
                    tcp_con_list.append(datalist.strip().split())
                    #client_ip_list.append(datalist[5].split(':')[0])
                print("tcp_con_list:", tcp_con_list)

                for list_item in tcp_con_list:
                    client_ip_list.append(list_item[5].split(":")[0])
                print("client_ip_list", client_ip_list)
                #客户IP列表
                list_set = set(client_ip_list)
                for item in list_set:
                    connect_count = client_ip_list.count(item)
                    check_result_list = []
                    #客户连接数超过限制报警
                    if connect_count >= single_user_limit:
                        #client_con_check = False
                        check_result_list.append(0)
                        msg = "Error:客户连接数超限，Server[%s] port[%s] client_ip[%s] tcp 连接数为：[%d]" \
                            % (hostip, tcp_port, item, connect_count)
                        logger.error(msg)
                        ct.send_sms_control('NoLimit', msg)
                    else:
                        #client_con_check = True
                        check_result_list.append(1)
                        msg = "Server[%s] port[%s] client_ip[%s] has tcp connect count [%d]" \
                            % (hostip, tcp_port, item, connect_count)
                        logger.info(msg)
                    client_con_verify = (sum(check_result_list)==len(check_result_list))

                self.single_info_verify = total_count_verify and client_con_verify
                    # msg = "error: " + hostip + " Have core file:" + datalist 
                    # ct.write_log(error_log_file, msg)
                    # logger.warning(msg)
                
                # sms_msg = "error: " + hostip + " 有core文件，请检查服务器文件"
                # logger.error(sms_msg)
                # ct.send_sms_control("core", sms_msg)

            msg = "tcp connect Check Result: " + str(self.single_info_verify)
            logger.info(msg)
