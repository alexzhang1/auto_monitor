# -*- coding: utf-8 -*-
"""
Created on 2019-11-21 13:43:46

@author: zhangwei
@comment: mysql公用函数  
"""

import pymysql
import pandas as pd
import numpy as np
import datetime as dt
import threading
import platform
import logging

logger = logging.getLogger('main.mysql_tools_class')



class mysql_tools:

    def __init__(self, db_info):
        self.host = db_info[0]
        self.user = db_info[1]
        self.passwd = db_info[2]
        self.db_name = db_info[3]
        self.port = db_info[4]


        try:
	    #     self.conn = pymysql.connect(host = self.host, \
	    #                         user = self.user, \
	    #                         passwd = self.passwd, \
	    #                         port = self.port, \
	    # #                         db = "test_db", \
	    #                         local_infile=1)
	        self.conn = pymysql.connect(host = self.host, \
	                            user = self.user, \
	                            passwd = self.passwd, \
	                            port = self.port,\
                                local_infile=1)
        except Exception as e:
            print('connect exception!')
            print(str(e))
        self.conn.set_charset('utf8mb4')  
        self.cursor = self.conn.cursor()
        self.cursor.execute('SET NAMES utf8mb4;')  
        self.cursor.execute('SET character_set_connection=utf8mb4;')  
        #return (cursor,conn)


    def execute_sql(self, sql):
        #(cursor,conn) = self.connect_mysql()
        try:
            print(sql)
            #print("self.cconn:",self.conn)
            self.cursor.execute(sql)
            self.conn.commit()
            print('...execute successfull!')
        except Exception as e:
            self.conn.rollback()
            # print('...have problem, already rollback!')
            # print(e)
            logger.error('...have problem, already rollback!', exc_info=True)
        self.conn.close()


    def fetchall_sql(self,sql):
        #(cursor, conn) = self.connect_mysql()
        try:
            print(sql)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            print('...execute successfull!')
        except Exception as e:
            self.conn.rollback()
            # print('...have problem, already rollback!')
            # print(e)
            logger.error('...have problem, already rollback!', exc_info=True)
        self.conn.close()
        return res


    def remove_index(self, csv_file):
        df = pd.DataFrame.from_csv(csv_file)
        df.to_csv(csv_file, encoding='utf-8', index=False)


    def retrieve_column_name(self, table_name):
        sql = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='" + self.db_name + "' AND `TABLE_NAME`='" + table_name + "';"        
        res = self.fetchall_sql(sql)
        return [res[i][0] for i in np.arange(len(res))]

        
    def load_table_commend_gen(self, csv_file_name, tb_name):
        template_file = './table_template/' + tb_name + '.csv'
        temp = pd.read_csv(template_file, index_col=None)
        #加上dtype=object不会将字符前面的00去掉了
        temp_data = pd.read_csv(csv_file_name,dtype=object)
        #temp_data.fillna(-999, inplace=True)
        temp_data = temp_data[temp.var_name]
        temp_file = './table_template/' + tb_name + '_temp.csv'
        temp_data.to_csv(temp_file, index=False, encoding='utf-8')
        print(temp_data)
        # Note: Windows下换行符为'\r\n'，Linux系统下换行符为'\n',Mac系统里，每行结尾是“<回车>”,即'\r'
        sysstr = platform.system()
        if sysstr == "Windows":
            commend =   "LOAD DATA LOW_PRIORITY LOCAL INFILE '" + temp_file + "' " + \
                        "REPLACE INTO TABLE " + self.db_name + '.' +  tb_name + \
                        """ CHARACTER SET utf8mb4 
                        FIELDS TERMINATED BY \',\' 
                        LINES TERMINATED BY '\r\n' 
                        IGNORE 1 LINES;"""
        #Linux换行'\n',暂时不考虑Mac系统
        else:
            #commend =   "LOAD DATA LOW_PRIORITY LOCAL INFILE 'temp.csv' " + \
            commend =   "LOAD DATA LOW_PRIORITY LOCAL INFILE '" + temp_file + "' " + \
                        "REPLACE INTO TABLE " + self.db_name + '.' +  tb_name + \
                        """ CHARACTER SET utf8mb4 
                        FIELDS TERMINATED BY \',\' 
                        LINES TERMINATED BY '\n' 
                        IGNORE 1 LINES;"""

        return commend
        
        
    def retrieve_table(self, table_name, time_name, start_date, end_date):
        sql = 'SELECT * FROM ' + self.db_name + '.' + table_name + ' WHERE ' + time_name + " >='" + \
        start_date + "' AND " + time_name + " <='" + end_date + "'"
        
        tp_table = self.fetchall_sql(sql)
        df_table = pd.DataFrame(list(tp_table))
        
        temp_table = 'D:/work/ths_work/local_work/MySQL/table_template/' + table_name + '.csv'
        table_infor = pd.DataFrame.from_csv(temp_table, index_col=None)
        df_table.columns = table_infor.var_name

        return df_table
    #def get_db_data(sql):
    #    (cursor, conn) = connect_mysql()
    #    try:
    #        print(sql)
    #        cursor.execute(sql)
    #        res = cursor.fetchall()
    #        des = cursor.description
    #        print('...execute successfull!')
    #    except Exception as e:
    #        conn.rollback()
    #        print('...have problem, already rollback!')
    #        print(e)
    #    conn.close()
    #    return res,des


    def get_db_df(self,sql):
        #(cursor, conn) = self.connect_mysql()
        try:
            print(sql)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            des = self.cursor.description
            print('...execute successfull!')
        except Exception as e:
            self.conn.rollback()
            # print('...have problem, already rollback!')
            # print(e)
            logger.error('...have problem, already rollback!', exc_info=True)
        #conn.close()
        
        db_columns = list(zip(*des))[0]
        db_df = pd.DataFrame(list(res), columns=db_columns)
        return db_df

    #检查mysql数据库的local_infile参数
    def get_local_infile_value(self):
        local_para_sql = "show global variables like 'local_infile';"
        #logger.info(file_sql)
        db_df = self.get_db_df(local_para_sql)
        print("db_df:",db_df)
        print("local_infile[value]:",db_df['Value'][0])
        local_file_msg = "local_infile 的值为 %s" % db_df['Value'][0]
        logger.info(local_file_msg)
        return db_df['Value'][0]

    def ms_close(self):
        self.conn.close()