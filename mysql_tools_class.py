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



class mysql_tools:

    def __init__(self, db_info):
        self.host = db_info[0]
        self.user = db_info[1]
        self.passwd = db_info[2]
        self.db_name = db_info[3]
        self.port = db_info[4]

        self.conn = pymysql.connect(host = self.host, \
                            user = self.user, \
                            passwd = self.passwd, \
                            port = self.port, \
    #                         db = "test_db", \
                            local_infile=1)
        self.conn.set_charset('UTF8MB4')  
        self.cursor = self.conn.cursor()
        self.cursor.execute('SET NAMES utf8;')  
        self.cursor.execute('SET character_set_connection=utf8;')  
        #return (cursor,conn)


    def execute_sql(self, sql):
        #(cursor,conn) = self.connect_mysql()
        try:
            print(sql)
            self.cursor.execute(sql)
            self.conn.commit()
            print('...execute successfull!')
        except Exception as e:
            self.conn.rollback()
            print('...have problem, already rollback!')
            print(e)
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
            print('...have problem, already rollback!')
            print(e)
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
        temp_data = pd.read_csv(csv_file_name)
        temp_data.fillna(-999, inplace=True)
        temp_data = temp_data[temp.var_name]
        temp_data.to_csv('temp.csv', index=False, encoding='utf-8')
        print(temp_data)
        commend =   "LOAD DATA LOW_PRIORITY LOCAL INFILE 'temp.csv' " + \
                    "REPLACE INTO TABLE " + self.db_name + '.' +  tb_name + \
                    """ CHARACTER SET utf8 
                    FIELDS TERMINATED BY \',\' 
                    LINES TERMINATED BY '\r\n' 
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
            print('...have problem, already rollback!')
            print(e)
        #conn.close()
        
        db_columns = list(zip(*des))[0]
        db_df = pd.DataFrame(list(res), columns=db_columns)
        return db_df

    def ms_close(self):
        self.conn.close()