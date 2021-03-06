# -*- coding: utf-8 -*-
"""
Created on Wed May 29 14:18:45 2019

@author: zhangwei
@comment: 数据库监控，盘前监控表的记录条数，日期字段，金额字段，匹配csv文件2个字段值比较，
盘前加入了检查KHXX，csv上场文件和数据库的对比，比较客户资金是否一致，不一致检查出入金记录和冻结资金冻结手续费。
盘中检查表数据是否增长；
需要配置好table_check.json配置文件。
"""

#import pymssql
#import paramiko
import mssql_tools as mt
import common_tools as ct
import datetime as dt
import time
import json
import os
import logging
import csv
import threading
import platform
import getopt
import sys
import pandas as pd
#import numpy as np
import math
#import decimal
#reload(sys)
#sys.setdefaultencoding('utf-8')



logger = logging.getLogger()
ndates = dt.datetime.now().strftime("%Y%m%d")

class MyThread(threading.Thread):

    def __init__(self,func,args,name=''):
        threading.Thread.__init__(self)
        self.name=name
        self.func=func
        self.args=args
    
    def run(self):
        #python3不支持apply
#        apply(self.func,self.args)
        self.result = self.func(*self.args)
        
    def get_result(self):
        try:
            return self.result
        except Exception:
            return None



'''
check table count,和json配置文件中的info["countcheck"]["count"]对比。
'''

def check_table_count(info, cursor, conn):
    
    serverip = info["serverip"]
#    servername = info["servername"]
    cclists = info["countcheck"]   
    try:
        for ccdict in cclists:
            tablename = ccdict["tablename"]
            count = int(ccdict["count"])
        
            sql = "SELECT count(*) FROM " + tablename
            (res,des) = mt.only_fetchall(cursor, conn, sql)       
            result_count = res[0][0]
            logger.debug("result_count: %d", result_count)
            if result_count == count:
                logger.info("Ok: Check DBserver: %s table: %s count: %d" % (serverip, tablename, result_count))
                check_flag = True
            else:
                logger.error("Failed: Check DBserver: %s table: %s count: %d not equal %d" % (serverip, tablename, result_count, count))
                check_flag = False
                return check_flag
    except Exception:
        logger.error('Faild to check count!', exc_info=True)            
    return check_flag


'''
check increase count，判断记录条数是否增加，会和上次生成的临时文件countfile对比。
'''

def check_table_increase(info, cursor, conn):
    
    serverip = info["serverip"]
#    servername = info["servername"]
#    user = info["user"]
#    password = info["password"]
    dbname = info["dbname"]
    cclists = info["increasecheck"]   
    
    countfile = "./tempdata/" + serverip + "_" + dbname + "_count.json"
    try:
        if os.path.exists(countfile):
            with open(countfile, 'r') as f:
                countdict = json.load(f)
                logger.debug("countdict")
                logger.debug(countdict)
        else:
            countdict = {}       
            for dic in cclists:
                countdict[str(dic['tablename'])] = 0
            
            logger.debug("countdict")
            logger.debug(countdict)
        for ccdict in cclists:            
            tablename = ccdict["tablename"]
            precount = countdict[str(tablename)]
            nowcount = 0
        
            sql = "SELECT count(*) FROM " + tablename
            (res,des) = mt.only_fetchall(cursor, conn, sql)       
            nowcount = res[0][0]
            logger.debug("nowcount: %d", nowcount)
            countdict[str(tablename)] = nowcount
            if nowcount > precount:
                logger.info("Ok: Check DBserver: %s increase table : %s nowcount: %d precount: %d" % (serverip, tablename, nowcount, precount))
                check_flag = True
            else:
                logger.error("Failed: Check DBserver: %s not increase table: %s nowcount: %d precount %d" % (serverip, tablename, nowcount, precount))
                check_flag = False
                return check_flag
    except Exception:
        logger.error('Faild to check increase!', exc_info=True)
    finally:
        json_str = json.dumps(countdict, indent=4)
        with open(countfile, 'w') as json_file:
            json_file.write(json_str)        
    return check_flag


'''
check money >0，根据json配置文件中info["moneycheck"]设置的表和字段进行比较是否字段大于0，支持多个字段。
'''        
 
def check_money_value(info, cursor, conn):
    
    serverip = info["serverip"]
#    servername = info["servername"]
    cclists = info["moneycheck"]
    try:
        for ccdict in cclists:
            tablename = ccdict["tablename"]
            fieldsstr = ccdict["fields"]
            logger.debug("fieldsstr:" + fieldsstr)
            sql = "SELECT " + fieldsstr + " FROM " + tablename
            (res,des) = mt.only_fetchall(cursor, conn, sql)   
            if res == [] or res == None:
                logger.info("Result is Null")
                check_flag = False
            else:          
                for item in res:
                    logger.debug(item)
                    for iitem in item:                        
                        if iitem >= 0:
                            logger.debug("Ok: Check DBserver: %s table: %s moneyfields: %s" % (serverip, tablename, fieldsstr))
                            check_flag = True
                        else:
                            logger.error("Failed: Check DBserver: %s table: %s Money Value: %d is minus" % (serverip, tablename, iitem))
                            logger.error(item)
                            check_flag = False
                            return check_flag
    except Exception:
        logger.error('Faild to check money!', exc_info=True)            
    return check_flag


'''
check TradingDay，根据json配置文件中的info["datecheck"]配置信息，进行当天交易日期的校验。
'''
def check_date_value(info, cursor, conn):
    
    serverip = info["serverip"]
#    servername = info["servername"]
#    ndates = dt.datetime.now().strftime("%Y%m%d")
    cclists = info["datecheck"]
    try:
#            filedlist=[]
        for ccdict in cclists:
            tablename = ccdict["tablename"]
            #only one col
            fieldsstr = ccdict["field"]
            logger.debug("fieldsstr:" + fieldsstr)
#            fieldsstr = ",".join(filedlist)
            sql = "SELECT " + fieldsstr + " FROM " + tablename
#            print "sql:", sql
            (res,des) = mt.only_fetchall(cursor, conn, sql)   
            if res == [] or res == None:
                logger.info("Result is Null")
                check_flag = False
            else:          
                for item in res:
                    logger.debug(item)  
                    if item[0] == ndates :
#                    if '20190717' == ndates :
                        logger.debug("Ok: Check DBserver: %s table: %s datefields: %s values: %s" % (serverip, tablename, fieldsstr, item[0]))
                        check_flag = True
                    else:
                        logger.error("Failed: Check DBserver: %s table: %s datefields: %s values: %s" % (serverip, tablename, fieldsstr, item[0]))
                        check_flag = False
                        return check_flag
    except Exception:
        logger.error('Faild to check TradingDay!', exc_info=True)            
    return check_flag
 
'''
check local csv file ,just check 2 values of csv，只对2个字段进行校验。
'''
def check_records_value(info, cursor, conn):

    serverip = info["serverip"]
#    servername = info["servername"]
    cclists = info["valuecheck"]
    try:
        for ccdict in cclists:
            tablename = ccdict["tablename"]
            fieldsstr = ccdict["feilds"]
            csvfile = ccdict["filename"]
            condition = ccdict["condition"]
            logger.debug("fieldsstr:" + fieldsstr)
            
            csvlist = [] #20190617-初始化应放在外面
            with open('./config/' + csvfile,'r') as csvFile:
                reader = csv.reader(csvFile)
#                csvlist = [] #20190617
                for row in reader:
                    csvlist.append(row)
            logger.debug("csvlist:")
            logger.debug(csvlist)

            sql = "SELECT " + fieldsstr + " FROM " + tablename + condition
            (res,des) = mt.only_fetchall(cursor, conn, sql)
            res_list=[]
            db_columns = list(zip(*des))[0]
            temlist = [str(i) for i in db_columns]

            res_list.append(temlist)
            if res == [] or res == None:
                logger.info("Result is Null")
                check_flag = False
            else:
                for item in res:
                    res_list.append(item)
                logger.debug("res_list:")
                logger.debug(res_list)
                find_flag=[]
                for csvl in csvlist[1:]:
                    for resl in res_list[1:]:
#                        print("type", resl[1], csvl[1] )
                        if resl[0] == csvl[0] and int(resl[1]) == int(csvl[1]):
                            find_flag.append(1)
                logger.debug("find_Flag:")
                logger.debug(find_flag)
                if len(find_flag) == len(csvlist[1:]) :
                    logger.info("Ok: Check DBserver: %s csvfile: %s is ok" % (serverip, csvfile))
                    check_flag = True
                else:
                    logger.error("Failed: Check DBserver: %s csvfile: %s is failed" % (serverip, csvfile))
                    logger.error("res_list:")
                    logger.error(res_list)
                    check_flag = False
                    return check_flag
    except Exception:
        logger.error('Faild to check csvfile!', exc_info=True)            
    return check_flag
       

def covervalue(x):
#    print "x:", x, math.isnan(x)
    if math.isnan(x):
        return 0
    else:
        return float(x)


#取得客户的出让金转账记录总额
def get_cust_TransferMoney(info, AccountID, cursor, conn):
    
    dbname = info["dbname"]
    
#    AccountID = '38200001111801'
#    sql = "SELECT [AccountID],[PreDeposit],[UsefulMoney] FROM [download].[dbo].[t_TradingAccount] WHERE AccountID = " + AccountID
    sql = "SELECT [AccountID],[TransferDirection],[Amount] \
        FROM [" + dbname + "].[dbo].[t_FundTransferDetail] WHERE AccountID = " + "'" + AccountID + "'" + " And TransferStatus = 1"
    logger.debug("sql:" + sql)
    (res,des) = mt.only_fetchall(cursor, conn, sql)
    db_columns = list(zip(*des))[0]
    temlist = [str(i) for i in db_columns]
    logger.debug(temlist)
    logger.debug(res)
    
    TransferMoney = 0
    if len(res) != 0:
        Transfer_df = pd.DataFrame(res, columns=temlist)        
        for index, row in Transfer_df.iterrows():
            logger.debug(row)
            #入金Or出金
            if str(row['TransferDirection']) in ['2','5','7']:
                TransferMoney += row['Amount']
            elif str(row['TransferDirection']) in ['3','4','6']:
                TransferMoney -= row['Amount']
                
    logger.debug("InOutMoney:"+ str(TransferMoney))
    return float(TransferMoney)


'''
检查上场文件ZJXX.csv文件中的KYJE和数据库里的UsefulMoney进行比较，
如果不一样，对比客户出入金记录和冻结资金记录，再不匹配的话报警
'''
def check_remote_csv_ZJXX(info, cursor, conn):
    
#    server = '192.168.238.10'
#    user = 'sa'
#    password = '123.comA'
#    database = 'download'   
#    conn = pymssql.connect(server, user, password, database)    
#    cursor = conn.cursor()
#    sship = "192.168.238.7"
#    sshport = 22
#    sshuser = "trade"
#    sshpw = "trade"  
    
    dbname = info["dbname"]
    serverip = info["serverip"]
    sship = info['sship']
    sshport = int(info['sshport'])
    sshuser = info['sshuser']
    sshpw = info['sshpw']
    csv_remote_dir_i = info['csv_remote_dir']
    csv_remote_dir = csv_remote_dir_i.replace('{ndates}', ndates)
      
    check_flag = False
    #获取csv文件中的ZJZH,KYJE
    zjxxfile_path = csv_remote_dir + '/' + "VIP_ZJXX" + ndates + ".csv"
    command = "cat " + zjxxfile_path + " | awk -F\",\" \'{OFS=\",\";print $3,$7}\'"
#    zjxxfile_path = '/home/trade/temp/debugkhh.csv'
#    command = "cat " + zjxxfile_path + " | awk -F\",\" \'{OFS=\",\";print $1,$2}\'"
    logger.debug("command:" + command)
    sshClient = ct.sshConnect(sship, sshport, sshuser, sshpw)
    
    sshRes = []
    sshRes = ct.sshExecCmd(sshClient, command)
    ct.sshClose(sshClient)
    logger.debug("sshRes:")
    logger.debug(sshRes)
    #去掉'\r'
    tem_ssh=[]
    for item in sshRes:
        item = item.replace('\r', '')
        tem_ssh.append(item)
        
    if len(tem_ssh) != 0:
        zjxx_columns = tem_ssh[0].split(',')
        zjxx_data=[]
        for ssr in tem_ssh[1:]:
            lists = ssr.split(',')
#            print("lists:", lists)
            zjxx_data.append(lists)
        zjxx_df = pd.DataFrame(zjxx_data, columns=zjxx_columns)
        zjxx_df.set_index('ZJZH', inplace=True)
#        for index, row in zjxx_df.iterrows():
#            print index, row['KYJE']
        logger.debug("The csv KYJE:")
        logger.debug(zjxx_df)
        #把结尾是'11'的资金账号过滤掉
        index_filter = filter(lambda x: x[-2:] != '11', zjxx_df.index)
        logger.debug('index_filter:')
        logger.debug(index_filter)
        zjxx_filter_df = zjxx_df.loc[index_filter]
        logger.debug("zjxx_filter_df")
        logger.debug(zjxx_filter_df)
        #df.loc[df['column_name'] != some_value]
        
        #获取数据库数据       
        sql = "SELECT [AccountID],[UsefulMoney],[FrozenCash],[FrozenCommission] FROM [" + dbname + "].[dbo].[t_TradingAccount]"
        (res,des) = mt.only_fetchall(cursor, conn, sql)
#        res_list=[]
        db_columns = list(zip(*des))[0]
        temlist = [str(i) for i in db_columns]
        
        db_kyje_df = pd.DataFrame(res, columns=temlist)
        db_kyje_df = db_kyje_df.rename(columns = {'AccountID':'ZJZH'})
        db_kyje_df.set_index('ZJZH', inplace=True)
        
        #UserfulMoney列加上FrozenCash和FrozenCommission冻结字段
        db_kyje_df['UsefulMoney'] = db_kyje_df.apply(lambda x: x['UsefulMoney'] + x['FrozenCash'] + x['FrozenCommission'], axis=1)
        
        new_df = pd.merge(zjxx_filter_df, db_kyje_df, how='left', on='ZJZH')
        logger.debug("new_df:")
        logger.debug(new_df)
        new_df['KYJE'] = new_df['KYJE'].map(lambda x: round(float(x),2))
        new_df['UsefulMoney'] = new_df['UsefulMoney'].map(lambda x: round(covervalue(x),2))
        money_diff = new_df.diff(axis=1)
        #过滤出差别不为0的客户
        def_khje = money_diff.loc[money_diff['UsefulMoney'] != 0]        
        if len(def_khje) != 0:
            check_count = 0
            def_khje = def_khje.drop(columns='KYJE')
            def_khje = def_khje.rename(columns = {'UsefulMoney':'InOutMoney'})
            for index, row in def_khje.iterrows():
                logger.info("UsefulMoney not equal KYJE, the ZJZH is: %s, Difference is: %.2f" % (index, round(row['InOutMoney'], 2)))
                TransferMoney = round(get_cust_TransferMoney(info, index, cursor, conn), 2)
                logger.info("ZJZH %s transfer money is: %.2f" % (index, TransferMoney))
                final_diff = TransferMoney - row['InOutMoney']
                #因为每笔委托会冻结0.1元，不体现在总冻结里,额外冻结资金设置一个数，小于这个数不报警，认为这个是被委托冻结了。
                extra_frozen = 10.0
                #比较资金差额和出入金，一致的话则没有问题。 
                if final_diff == 0:
                    logger.info("The Difference equal to TransferMoney")
                    check_count += 1
                #因为每笔委托会冻结0.1元，不体现在总冻结里，所以要单独处理。
                elif final_diff > 0 and final_diff < extra_frozen:
                    logger.info("The Difference extra_frozen %.2f" % final_diff)
                    check_count += 1
                else:
                    logger.error("error:The csv ZJZH: %s KYJE not equal to DB: %s UsefulMoney,Difference is: %.2f" % (index, serverip, round(row['InOutMoney'], 2)))
            check_flag = (check_count == len(def_khje))
        else:
            logger.info("ok:The csv KYJE equal to DB UsefulMoney")
            check_flag = True

    else:
        logger.error("error: Failed to get remote server: %s csv file: %s, please check the config!" % (sship, zjxxfile_path))
    
    return check_flag



'''
盘前的检查项
'''
def before_trade_monitor(info):
    
    try:               
        server = info["serverip"]
        user = info["user"]
        password = info["password"]
        dbname = info["dbname"] 
#        servername = info["servername"]           
        db_info = [server, user, password, dbname]
        (cursor, conn) = mt.connect_mssql(db_info)
        
        if cursor != None:           
            count_check = check_table_count(info, cursor, conn)
            money_check = check_money_value(info, cursor, conn)
            date_check = check_date_value(info, cursor, conn)
#            date_check = True
            csv_check =  check_records_value(info, cursor, conn)
            csv_ZJXX_check = check_remote_csv_ZJXX(info, cursor, conn)
            check_result = count_check and money_check and date_check and csv_check and csv_ZJXX_check
#            check_result = csv_ZJXX_check
        else:
            logger.error('Can not get cursor!')
            check_result = False
            
        if check_result:
            logger.info("OK: before trade check database: %s success!", server)
        else:
            logger.error("Failed: before trade check database: %s failed!", server)
               
    except Exception:
        logger.error('Faild to check database before trade!', exc_info=True)
        check_result = False
    finally:
        conn.close()
        return check_result


'''
交易中检查项
'''
def trading_monitor(info):
    
    try:               
        server = info["serverip"]
        user = info["user"]
        password = info["password"]
        dbname = info["dbname"]    
#        servername = info["servername"]        
        db_info = [server, user, password, dbname]
        (cursor, conn) = mt.connect_mssql(db_info)
        
        if cursor != None:           
            check_result = check_table_increase(info, cursor, conn)
            logger.debug('increase_check:' + str(check_result))
        else:
            logger.error('Can not get cursor!')
            check_result = False
            
        if check_result:
            logger.info("OK: database: %s trading check success!", server)
        else:
            logger.error("Failed: database: %s trading check failed!", server)
               
    except Exception:
        logger.error('Faild to check database trading!', exc_info=True)
        check_result = False
    finally:
        conn.close()
        return check_result


def main(argv):    
    
    try:       
        yaml_path = './config/db_check_logger.yaml'
        ct.setup_logging(yaml_path)
        
        with open('./config/table_check.json', 'r') as f:
            Jsonlist = json.load(f)
            logger.debug(Jsonlist)      
        
        #init interval
        inc = 600
        modul = ''
        try:
            opts, args = getopt.getopt(argv,"hl:e:",["loopsecends=", "excute="])
        except getopt.GetoptError:
            print('db_monitor.py -l <loopsecends> -e <excute>')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('db_monitor.py -l <loopsecends> -e <excute>\n \
                    loopsecends=0 means no loop and just run once.\n \
                    loopsecends=N means loop interval is N second. \n \
                    (default:python db_monitor.py) means loop interval is 600 seconds. \n \
                    excute=before means excute the before trade db monitor. \n \
                    excute=trading means excute the trading db monitor. \n \
                    excute is Null means excute before and trading db monitor.'  )           
                sys.exit()
            elif opt in ("-l", "--loopsecends"):
                inc = int(arg)
            elif opt in ("-e", "--excute"):
                modul = arg
            logger.info('interval is: %d' % inc)
            logger.info('modull is:s: %s' % modul)
        if inc == 0:
            if modul == 'before' or modul == '':
                #before trade montior     
                logger.info("Start to excute the before trade monitor")
                thrlist = range(len(Jsonlist))
                threads=[]
                for (i,info) in zip(thrlist, Jsonlist):
                    #print("alltask.__name__:", alltask.__name__)
                    t = MyThread(before_trade_monitor,(info,),before_trade_monitor.__name__ + str(i))
                    threads.append(t)
                    
                for i in thrlist:
                    threads[i].start()
                for i in thrlist:       
                    threads[i].join()
                    threadResult = threads[i].get_result()
                    sysstr = platform.system()
                    if (not threadResult) and (sysstr == "Windows"):
                        ct.readTexts("Database trade before Worning") 
            if modul == 'trading' or modul == '':
                #trading monitor       
                #delete all tempdata/*.json 
                if os.path.isdir("./tempdata"):
                    for filename in os.listdir('./tempdata'):
                        os.remove('./tempdata/' + filename)
                else:
                    os.mkdir("./tempdata")
                logger.info("Start to excute the trading_monitor")          
                thrlist = range(len(Jsonlist))
                threads=[]
                for (i,info) in zip(thrlist, Jsonlist):
                    t = MyThread(trading_monitor,(info,),trading_monitor.__name__ + str(i))
                    threads.append(t)
#                print "thrcouat3:", threading.active_count()
                for i in thrlist:
                    threads[i].start()
                for i in thrlist:       
                    threads[i].join()
                    trading_check = threads[i].get_result()
                    if (not trading_check) and (sysstr == "Windows"):
                        ct.readTexts("Database trading Worning")
        elif inc > 20:
            #before trade montior  
            logger.info("Start to excute the before trade monitor")
            thrlist = range(len(Jsonlist))
            threads=[]
            for (i,info) in zip(thrlist, Jsonlist):
                #print("alltask.__name__:", alltask.__name__)
                t = MyThread(before_trade_monitor,(info,),before_trade_monitor.__name__ + str(i))
                threads.append(t)
                
            for i in thrlist:
                threads[i].start()
            for i in thrlist:       
                threads[i].join()
                threadResult = threads[i].get_result()
                sysstr = platform.system()
                if (not threadResult) and (sysstr == "Windows"):
                    ct.readTexts("Database trade before Worning")       
#            print "thrcount:", threading.active_count() 
                    
            #trading monitor       
            #delete all tempdata/*.json 
            if os.path.isdir("./tempdata"):
                for filename in os.listdir('./tempdata'):
                    os.remove('./tempdata/' + filename)
            else:
                os.mkdir("./tempdata")                       
            while True:  
#            while False: 
                if (ct.trade_check()):
                    logger.info("Start to excute the trading_monitor")          
                    thrlist = range(len(Jsonlist))
                    threads=[]
                    for (i,info) in zip(thrlist, Jsonlist):
                        t = MyThread(trading_monitor,(info,),trading_monitor.__name__ + str(i))
                        threads.append(t)
                    for i in thrlist:
                        threads[i].start()
                    for i in thrlist:       
                        threads[i].join()
                        trading_check = threads[i].get_result()
                        if (not trading_check) and (sysstr == "Windows"):
                            ct.readTexts("Database trading Worning")
#                    print "thrcouat3:", threading.active_count()
                    time.sleep(inc-20)
                    if (ct.time_check('15:00', '15:12')):
                        logger.info("exit to monitor")
                        break
                else:
                    logger.info("It's not time to trading monitor")
                time.sleep(20)
        else:
            logger.error("Input parameter error: The interval must greater than 20!")
    except Exception:
        logger.error('Faild to run monitor db!', exc_info=True)
    finally:
        for handler in logger.handlers:
            logger.removeHandler(handler)

           
if __name__ == '__main__':
        main(sys.argv[1:])    
