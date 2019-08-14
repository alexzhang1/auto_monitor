# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 15:48:34 2019

@author: zhangwei
@comment: 支持从windows到linux的上传下载文件，可以上传文件目录也可以上传单个文件;
          下载远程Linux服务器的文件，支持单个和目录递归下载，会保存到指定目录下以服务器IP地址命名的文件目录下。
"""

import paramiko
#import datetime
import os
import stat
import time
import re
import sys
import getopt
import logging
#import logging.config
#import yaml
import common_tools as ct
#reload(sys)
#sys.setdefaultencoding('utf-8')



logger = logging.getLogger()

# ------获取远端linux主机上指定目录及其子目录下的所有文件------
def get_all_files_in_remote_dir(sftp, remote_dir):
    # 保存所有文件的列表
    all_files = []

    # 去掉路径字符串最后的字符'/'，如果有的话
    if remote_dir[-1] == '/':
        remote_dir = remote_dir[0:-1]

    # 获取当前指定目录下的所有目录及文件，包含属性值，不会取"."和".."开头的文件
    try:
        files = sftp.listdir_attr(remote_dir)
    except Exception as e:
        print("get file_attr exception: ", str(e))
    for x in files:
        # remote_dir目录中每一个文件或目录的完整路径
        filename = remote_dir + '/' + x.filename
        # 如果是目录，则递归处理该目录，这里用到了stat库中的S_ISDIR方法，与linux中的宏的名字完全一致
        if stat.S_ISDIR(x.st_mode):
            all_files.extend(get_all_files_in_remote_dir(sftp, filename))
        else:
            all_files.append(filename)
    return all_files


'''
上传本地目录下(./upload)的所有文件和目录到远程服务器上，远程服务器没有目录的话会自动创建。
如果是sh结尾的文件会执行chmod 755 *.sh给权限。
支持单个文件上传。
'''
def upload(local_upload, remote_dir, hostip, port, username, password, singlefile):
    try:
        t = paramiko.Transport((hostip, port))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        logger.info('upload file is starting')
        for root, dirs, files in os.walk(local_upload):
            if singlefile != '':
                #D:/my_project/python/auto_monitor/upload,['test2', 'test3'],['monitor_log_2019-05-22.txt', 'monitor_log_2019-05-23.txt', 'monitor_log_2019-05-24.txt', 'monitor_log_2019-05-30.txt']
                root = local_upload
                dirs = []
                files = [singlefile]
            logger.debug('all: %s, %s, %s' % (root, dirs, files))
            for filespath in files:
                local_file = os.path.join(root, filespath)
                logger.debug(11, '%s, %s, %s, %s' % (root, filespath, local_file, local_upload))
                if os.path.isfile(local_file):
                    a = local_file.replace(local_upload, '').replace('\\', '/').lstrip('/')
                    logger.debug('01', a, '[%s]' % remote_dir)
                    remote_file = os.path.join(remote_dir, a).replace('\\', '/')
                    logger.debug(22, remote_file)
                    try:
                        logger.info(u"文件 %s 上传到 %s" % (local_file, remote_file))
                        sftp.put(local_file, remote_file)                      
                        suffix = remote_file.split('.')[-1]
                        if suffix == 'sh':
                            logger.debug("sh_file: " + remote_file)
                            try:
                                sftp.chmod(remote_file, 0o755)
                            except Exception as e :
                                logger.error("execute chmod error:" + str(e))
                    except Exception as e:
                        logger.error("execute put error:" + str(e))
                        sftp.mkdir(os.path.split(remote_file)[0])
                        logger.info(u"新建目录后再上传,文件 %s 上传到 %s" % (local_file, remote_file))
                        sftp.put(local_file, remote_file)
                        suffix = remote_file.split('.')[-1]
                        if suffix == 'sh':
                            logger.debug("sh: " + remote_file)
                            try:
                                sftp.chmod(remote_file, 0o755)
                            except Exception as e :
                                logger.error("execute chmod error:" + str(e))
                        
                else:
                    logger.error(local_file + ' is not exist!')
                    return 0
            for name in dirs:
                local_path = os.path.join(root, name)
                logger.debug(0, local_path, local_upload)
                a = local_path.replace(local_upload, '').replace('\\', '/').lstrip('/')
                logger.debug(1, a)
                logger.debug(1, remote_dir)
                # remote_path = os.path.join(remote_dir, a).replace('\\', '/')
                remote_path = remote_dir + '/' + a
                logger.debug("33 " + remote_path)
                try:
                    sftp.mkdir(remote_path)
                    logger.debug("44, mkdir path %s" % remote_path)
                except Exception as e:
                    logger.debug("55, " + str(e))
        logger.info(hostip + ' Upload file success!')
        t.close()
    except Exception as e:
        logger.error(hostip + " Upload Exception: " + str(e))


'''
下载远程服务器的文件到本地目录，默认是./download路径，生成远程服务器IP地址的目录来区分从多个服务器下载。
目录下载时不会取"."和".."开头的文件。
支持单个文件下载，可以下载"."和".."开头的文件。
'''
def download(local_download, remote_dir, hostip, port, username, password, singlefile):
    try:
        local_download = local_download + os.sep + hostip
        t = paramiko.Transport((hostip, port))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        logger.info(hostip + ' download file starting')
#        files = sftp.listdir(remote_dir)  # 下载多个文件
        if singlefile != '':
            all_files = [remote_dir + '/' + singlefile]
        else:
            #获取linux下所有文件名
            all_files = get_all_files_in_remote_dir(sftp, remote_dir)
        logger.debug("all_files:", all_files)
        #获得远程目录层数
        i = len(re.split('[:/]', remote_dir))
        # 依次get每一个文件
        for x in all_files:            
            filepath = re.split('[:/]', x)
            logger.debug("filepath:", filepath)
            filename = x.split('/')[-1]
            local_path = local_download + os.sep + '/'.join(filepath[i:-1])
            logger.debug("local_path:", local_path)
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            local_filename = os.path.join(local_path, filename)
            logger.debug("local_filename:", local_filename)
            logger.info(u'文件 %s 下载中...' % filename)
            sftp.get(x, local_filename)
        logger.info(hostip + ' Download file success!')
        t.close()   
    except Exception as e:
        logger.error(hostip + " Download Exception: " + str(e))



def main(argv):
  
    try:
        yaml_path = './config/file_transfer_logger.yaml'
        ct.setup_logging(yaml_path)
        linuxInfo = ct.get_server_config('./config/file_transfer_config.txt')
        
        #读取输入参数
        ftmethod = ''
        windir = ''
        lindir = ''
        singlefile = ''
        try:
            opts, args = getopt.getopt(argv,"hm:w:l:f:",["method=", "windir=", "lindir=" ,"filename="])
        except getopt.GetoptError:
            print('file_transfer.py -m <method> -w <windir> -l <lindir> -f <filename>')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('file_transfer.py -m <method> -w <windir> -l <lindir> -f <filename>\n \
                    method=upload, means upload file from windows to linux.\n \
                    method=download, means download file from linux to windows. \n \
                    windir=dir, Window dir, The format like this: "D:\\my_project\\python". \n \
                    lindir=dir, Linux dir, The format like this: "/home/trade/temp". \n \
                    filename=filename, options- The single file to upload or download.'  )       
                sys.exit()
            elif opt in ("-m", "--method"):
                ftmethod = arg
            elif opt in ("-w", "--windir"):
                windir = arg
            elif opt in ("-l", "--lindir"):
                lindir = arg
            elif opt in ("-f", "--filename"):
                singlefile = arg

        
#            hostip = '192.168.238.7'
#            username = 'trade'
#            password = 'trade'
#            port = 22
#            local_upload = 'D:/my_project/python/auto_monitor/upload'
#            download_dir = 'D:/my_project/python/auto_monitor/download'
#            remote_dir = '/home/trade/temp'
            
          
#        singlefile = 'monitor_log_2019-05-30.txt'
#        ftmethod = 'download'
        if ftmethod == 'upload':
            for info in linuxInfo:               
                hostip = info[0]
                port = int(info[1])
                username = info[2]
                password = info[3]
                local_upload = info[5]
                local_download = info[6]
                remote_dir = info[7]
                
                if windir !='':
                    local_upload = windir
                if lindir !='':
                    remote_dir = lindir
                upload(local_upload, remote_dir, hostip, port, username, password, singlefile)
        elif ftmethod == 'download':
            for info in linuxInfo:               
                hostip = info[0]
                port = int(info[1])
                username = info[2]
                password = info[3]
                local_upload = info[5]
                local_download = info[6]
                remote_dir = info[7]
                
                if windir !='':
                    local_download = windir
                if lindir !='':
                    remote_dir = lindir
                download(local_download, remote_dir, hostip, port, username, password, singlefile)
        else:
            logger.error("method input is invalid! -m upload or -m download?")
        
    except Exception:
        logger.error('Faild to transfer file!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)




if __name__ == '__main__':
    print('start:%s' %time.ctime())
    main(sys.argv[1:])
    print('end:%s' %time.ctime())