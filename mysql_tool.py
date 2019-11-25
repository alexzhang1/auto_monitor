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





def connect_mysql(host,user,passwd,dbname,port):

    
    conn = pymysql.connect(host = host, \
                         user = user, \
                         passwd = passwd, \
                         port = port, \
#                         db = "test_db", \
                         local_infile=1)
    conn.set_charset('utf8')  
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')  
    cursor.execute('SET character_set_connection=utf8;')  
    return (cursor,conn)


def execute_sql(sql):
    (cursor,conn) = connect_mysql()
    try:
        print(sql)
        cursor.execute(sql)
        conn.commit()
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        print('...have problem, already rollback!')
        print(e)
    conn.close()


def fetchall_sql(sql):
    (cursor, conn) = connect_mysql()
    try:
        print(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        print('...have problem, already rollback!')
        print(e)
    conn.close()
    return res


def remove_index(csv_file):
    df = pd.DataFrame.from_csv(csv_file)
    df.to_csv(csv_file, encoding='utf-8', index=False)


def retrieve_column_name(db_name, table_name):
   sql = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='" + db_name + "' AND `TABLE_NAME`='" + table_name + "';"        
   res = fetchall_sql(sql)
   return [res[i][0] for i in np.arange(len(res))]

    
def load_table_commend_gen(csv_file_name, db_name, tb_name):
    template_file = 'D:/work/ths_work/local_work/MySQL/table_template/' + tb_name + '.csv'
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
    
    
def retrieve_table(db_name, table_name, time_name, start_date, end_date):
    sql = 'SELECT * FROM ' + db_name + '.' + table_name + ' WHERE ' + time_name + " >='" + \
    start_date + "' AND " + time_name + " <='" + end_date + "'"
    
    tp_table = fetchall_sql(sql)
    df_table = pd.DataFrame(list(tp_table))
    
    temp_table = 'D:/work/ths_work/local_work/MySQL/table_template/' + table_name + '.csv'
    table_infor = pd.DataFrame.from_csv(temp_table, index_col=None)
    df_table.columns = table_infor.var_name

    return df_table
    
    
def read_ths_status():
    with open("ths_status.txt", "r") as f:
        status = f.read()
    return status


def write_ths_status(status):
    with open("ths_status.txt", "w") as f:
         f.write(status)
    

def footprint(filename, content):
    ntime = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(filename, 'a+') as f:
        f.write(ntime + "::" + content)
        f.write('\n')
    

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


def get_db_df(sql):
    (cursor, conn) = connect_mysql()
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