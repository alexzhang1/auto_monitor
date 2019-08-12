# -*- coding: utf-8 -*-
"""
Created on Wed May 29 16:12:55 2019

@author: zhangwei
@comment: 连接sqlserver2008数据库，自定义的查询记录，并将结果保存到文件。
"""

import mssql_tools as mt
import common_tools as ct
import datetime as dt
import os
import csv
import codecs
import logging
#import logging.config
#import yaml
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')



#dbInfo = ct.get_server_config('./config/DBServer_config.txt')
ndates = dt.datetime.now().strftime("%Y%m%d")
ntimes = dt.datetime.now().strftime("%Y%m%d%H%M%S") 
    
cur_dir = os.getcwd().replace("\\","/") + "/"
#存放目录db_records

if os.path.exists("./db_records"):
    pass
else:
    os.mkdir('./db_records')
log_dir = cur_dir + "db_records/"
log_file = log_dir + "dbquery_log_" + ndates + '.txt'
logger = logging.getLogger()



def get_db_records(info):
    
    tablename = info[4]
    sql = info[5]
    records_file = log_dir + tablename + "_" + ndates + '.csv'

    #清空文件内容
    if os.path.exists(records_file):
        with open(records_file, "r+") as f:        
            f.seek(0)
            f.truncate()

#    sql = "SELECT [OrderLocalID], [OrderSysID] from \
#        dbo.t_SSEOrder ORDER BY OrderLocalID DESC"
#    sql = "SELECT UserID FROM dbo.t_User WHERE UserName = '张三'"

    (res,des) = mt.fetchall_sql(info, sql)
    if res == None or res == []:
        msg = "Failed to get records"
        logger.error(msg)
        ct.write_log(log_file, msg)
    else:
        logger.debug(res)
        db_columns = list(zip(*des))[0]
        logger.debug(db_columns)
            
        with codecs.open(filename=records_file, mode='w', encoding='utf-8') as f:
            write = csv.writer(f, dialect='excel')
            write.writerow(db_columns)
#            write.writerows(res)
            for item in res:
                logger.debug(item)
                write.writerow(item)
        
def main():
    try:
        info = ["192.168.238.10","sa","123.comA","download","dbo.t_User",\
                "SELECT UserID FROM dbo.t_User WHERE UserName = '张三'"]
        yaml_path = './config/mssql_records_logger.yaml'
        ct.setup_logging(yaml_path)
        get_db_records(info)
        logger.info("get records success!")
    except Exception:
        logger.error('Faild to get records!', exc_info=True) 
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)
#        logger.removeHandler(info_file_handler)
#        logger.removeHandler(console)



if __name__ == '__main__':
        main()    
