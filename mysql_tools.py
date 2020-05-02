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
import logging


logger = logging.getLogger('main.mssql_tools')

def connect_mysql(db_info):

    server = db_info[0]
    user = db_info[1]
    password = db_info[2]
    database = db_info[3]
    port = db_info[4]
    conn = pymysql.connect(host = server, \
                         user = user, \
                         passwd = password, \
                         port = port, \
#                         db = "test_db", \
                         local_infile=1)
    conn.set_charset('utf8')  
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')  
    cursor.execute('SET character_set_connection=utf8;')  
    return (cursor,conn)


def execute_sql(cursor, conn, sql):
    #(cursor,conn) = connect_mysql()
    try:
        logger.info(sql)
        cursor.execute(sql)
        conn.commit()
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        logger.error('...have problem, already rollback!', exc_info=True)
        #print(e)
    conn.close()


def fetchall_sql(cursor, conn, sql):
    #(cursor, conn) = connect_mysql()
    try:
        print(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        logger.error('...have problem, already rollback!', exc_info=True)
        #print(e)
    conn.close()
    return res


def remove_index(csv_file):
    df = pd.DataFrame.from_csv(csv_file)
    df.to_csv(csv_file, encoding='utf-8', index=False)

    
def load_table_commend_gen(csv_file_name, db_name, tb_name):
    template_file = './table_template/' + tb_name + '.csv'
    temp = pd.read_csv(template_file, index_col=None)
    temp_data = pd.read_csv(csv_file_name)
    temp_data.fillna(-999, inplace=True)
    temp_data = temp_data[temp.var_name]
    temp_data.to_csv('temp.csv', index=False, encoding='utf-8')
    #print(temp_data)
    commend =   "LOAD DATA LOW_PRIORITY LOCAL INFILE 'temp.csv' " + \
                "REPLACE INTO TABLE " + db_name + '.' +  tb_name + \
                """ CHARACTER SET utf8 
                FIELDS TERMINATED BY \',\' 
                LINES TERMINATED BY '\r\n' 
                IGNORE 1 LINES;"""
    return commend
    
    
        
def footprint(filename, content):
    ntime = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(filename, 'a+') as f:
        f.write(ntime + "::" + content)
        f.write('\n')
    

def get_db_df(sql, db_info):
    (cursor, conn) = connect_mssql(db_info)
    try:
        print(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        des = cursor.description
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        print('...have problem, already rollback!')
        print(e)
    conn.close()
    
    db_columns = list(zip(*des))[0]
    db_df = pd.DataFrame(list(res), columns=db_columns)
    return db_df