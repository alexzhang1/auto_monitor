#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   gen_InvestorExchFee.py
@Time    :   2020/08/21 14:43:41
@Author  :   wei.zhang 
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
import pandas as pd
from dbfread import DBF
import numpy as np
import json
import datetime as dt
import logging
import mysql_tools_class as myc
import common_tools as ct
import os

ndates = dt.datetime.now().strftime("%Y-%m-%d")
logger = logging.getLogger()

#后缀为数据日期，格式为 mdd 格式，其中 m 表示月，dd 表示日。当月份为 10、11、12 月时，m的值分别为‘a’、‘b’、‘c’
def shdbf_date_str(todays = dt.datetime.now().strftime("%Y%m%d")):
    m = str(hex(int(todays[4:6])))[-1:]
    sh_date_str = m + str(todays)[-2:]
    return sh_date_str

def szdbf_date_dir(todays = dt.datetime.now().strftime("%Y%m%d")):
    m = str(int(todays[4:6]))
    szdbf_date_dir = 'D-COM/File/' + todays[:4] + '-' + m + '-' + todays[-2:]
    return szdbf_date_dir

#红利征税数据表文件名
dbf_dir = '/home/trade/monitor_server/SSCC/'
#dbf_dir = './DBdata/InvestorExchFee/dbf_file/'
todays = dt.datetime.now().strftime("%Y%m%d")
sh_date_str = shdbf_date_str(todays)
szdbf_date_dir = szdbf_date_dir(todays)
#print("sh_date_str:",sh_date_str)
abcsj_dbf = dbf_dir + 'PROP/' + todays + '/abcsjjs588.' + sh_date_str
zsmx_dbf = dbf_dir + szdbf_date_dir +'/ZSMX' + str(todays)[-4:] + '.DBF'
csv_file_name = './DBdata/InvestorExchFee/InvestorExchFee_' + ndates + '_dbdata.csv'
null_investorID = []

info = ct.get_server_config('./config/mysql_config_sf.txt')
mysql_db_ip = info[0][0]
mysql_user = info[0][1]
mysql_passwd = info[0][2]
mysql_dbname = info[0][3]
#mysql_dbname = 'singular_field'
table_name = 't_CustShareholderInfo'
mysql_port = int(info[0][4])
mysqldb_info = [mysql_db_ip, mysql_user, mysql_passwd,mysql_dbname, mysql_port]
#print("mysqldb_info:",mysqldb_info)
#mysql_obj = myc.mysql_tools(mysqldb_info)


#数据库查询匹配inverstorID,匹配不到的话用'999999'替代
def get_investorID(shareholderID,mysql_obj):
    query_sql = "SELECT investorID FROM " + mysql_dbname + "." + table_name + " WHERE shareholderID = " + "'" + shareholderID + "';"
    res = mysql_obj.fetchall_sql_noclose(query_sql)
    if len(res) == 0:
        logger.error('shareholderID %s 没有匹配到对应的investorID' % shareholderID)
        investorID = '999999'
        null_investorID.append(shareholderID)
        #df = df.drop(index=(df.loc[(df['table']=='sc')].index))
        #InvestorExchFee_df.drop(index=(InvestorExchFee_df.loc[InvestorExchFee_df['shareholderID'] == shareholderID].index))
        
    else:
        investorID = res[0][0]
    return investorID


def map_to_investorID(InvestorExchFee_df):
    mysql_obj = myc.mysql_tools(mysqldb_info)
    #转换investorID
    InvestorExchFee_df['investorID'] = InvestorExchFee_df['investorID'].apply(get_investorID,args = (mysql_obj,))
    #管理mysql连接
    mysql_obj.ms_close
    InvestorExchFee_df = InvestorExchFee_df.drop(index=(InvestorExchFee_df.loc[InvestorExchFee_df['investorID'] == '999999'].index))
    return InvestorExchFee_df


def test_return(shareholderID,teststr):
    return '8888888' + shareholderID + teststr


def gen_ExchFee_csv():
    if os.path.isfile(abcsj_dbf):
        sh_table = DBF(abcsj_dbf)
    else:
        sh_table = None
    if os.path.isfile(zsmx_dbf):
        sz_table = DBF(zsmx_dbf)
    else:
        sz_table = None
    #exchangeID:1,shanghai;2,shenzhen
    #ZQLB:PT/JJ/GZ,普通，基金，国债
    db_columns = ['tradingDay', 'investorID', 'exchangeID', 'securityID','securityType','feeType','feeAmount']
    InvestorExchFee_df = pd.DataFrame(columns=db_columns)
    #处理上海数据
    if sh_table != None:
        sh_df = pd.DataFrame(iter(sh_table))
        sh_abcsj_col = ['TZLB','TZRQ','ZQZH','ZQDM','ZQLB','JE1']
        sh_pieces = sh_df[sh_abcsj_col].copy()
        sh_pieces.rename(columns={"TZLB": "feeType", "TZRQ": "tradingDay","ZQZH": "investorID","ZQDM": "securityID","ZQLB": "securityType","JE1": "feeAmount"},inplace = True)
        sh_len = len(sh_pieces)
        list_ex_id = ['SH']*sh_len
        sh_pieces.loc[:,'exchangeID'] = list_ex_id
        #sh_pieces['tradingDay'] = sh_pieces['tradingDay'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
        #sh_pieces['tradingDay'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
        #sh_pieces[db_columns]
        InvestorExchFee_df = InvestorExchFee_df.append(sh_pieces[db_columns],ignore_index=True)
    #处理深圳数据
    if sz_table != None:
        sz_df = pd.DataFrame(iter(sz_table))
        sz_zsmx_col = ['MXYWLB','MXZQDH','MXGDDM','MXFSJE','MXFSRQ']
        sz_pieces = sz_df[sz_zsmx_col].copy()
        sz_pieces.rename(columns={"MXYWLB": "feeType", "MXFSRQ": "tradingDay","MXGDDM": "investorID","MXZQDH": "securityID","MXFSJE": "feeAmount"},inplace = True)
        sz_len = len(sz_pieces)
        list_sz_id = ['SZ']*sz_len
        sz_pieces.loc[:,'exchangeID'] = list_sz_id
        sz_pieces.loc[:,'securityType'] = ''
        print(sz_pieces['tradingDay'][0])
        sz_pieces['tradingDay'] = sz_pieces['tradingDay'].apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:])
        print(sz_pieces['tradingDay'][0])
        InvestorExchFee_df = InvestorExchFee_df.append(sz_pieces[db_columns],ignore_index=True)

    InvestorExchFee_df = map_to_investorID(InvestorExchFee_df)
    # ppp = InvestorExchFee_df.loc[InvestorExchFee_df['investorID'] == '999999']
    # print("ppp:",ppp)
    #print(InvestorExchFee_df)
    InvestorExchFee_df.to_csv(csv_file_name, encoding='utf-8')


def insert_mysql(csv_file_name,template_name):
    logger.info("导入mysql....")
    mysql_obj = myc.mysql_tools(mysqldb_info)
    local_infile_value = mysql_obj.get_local_infile_value()
    #判断mysql参数是否打开允许导入文件
    if (local_infile_value == 'ON' and os.path.isfile(csv_file_name)):
        file_sql = mysql_obj.load_table_commend_gen(csv_file_name, template_name)
        logger.info(file_sql)
        res = mysql_obj.execute_sql(file_sql)
    else:
        local_file_msg = "Error，mysql导入csv失败，local_infile 的值为： %s" % local_infile_value
        ct.send_sms_control('NoLimit', local_file_msg, '13681919346')
        res = 0
    return res

def main():
    yaml_path = './config/gen_InvestorExchFee_logger.yaml'
    ct.setup_logging(yaml_path)
    #检查当天是否是交易日
    is_tradedate = ct.get_isTradeDate(ndates)
    if not is_tradedate:
        logger.info("当日是非交易日，程序退出")
        return 0
    #检查文件是否存在
    if not os.path.isfile(abcsj_dbf):  
        msg = "没有找到上海红利征税明细文件 %s" % abcsj_dbf         
        logger.error(msg)
        #ct.send_sms_control('NoLimit',msg)
    if not os.path.isfile(zsmx_dbf):  
        msg = "没有找到深圳红利征税明细文件 %s" % zsmx_dbf         
        logger.error(msg)
        #ct.send_sms_control('NoLimit',msg)

    if (os.path.isfile(abcsj_dbf) or os.path.isfile(zsmx_dbf)):
        gen_ExchFee_csv()
        res = insert_mysql(csv_file_name,'t_InvestorExchFee')
        if res :
            logger.info("插入mysql成功")
            ct.send_sms_control('NoLimit',"OK，导入红利征税表t_InvestorExchFee成功")
        else:
            logger.error("Error，插入mysql失败")
        logger.info("len(null_investorID):" + str(len(null_investorID)))
        if len(null_investorID) !=0:
            msg = "有没有匹配到对应的investorID的shareholderID数量为：%s" % str(len(null_investorID))
            logger.info(msg)
        else:
            logger.info('shareholderID全匹配成功')
    else:
        logger.error("当天红利征税明细dbf文件没有找到")



if __name__ == "__main__":
    main()