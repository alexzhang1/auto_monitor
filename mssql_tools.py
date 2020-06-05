# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:07:10 2019

@author: zhangwei
"""

import pymssql
import datetime as dt
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger('main.mssql_tools')


def connect_mssql(db_info):
    
    server = db_info[0]
    user = db_info[1]
    password = db_info[2]
    database = db_info[3]
  
#    server = '192.168.238.10'
#    user = 'sa'
#    password = '123.comA'
#    database = 'download'
    
    try:
        conn = pymssql.connect(server, user, password, database, login_timeout=5)
    #    conn.set_charset('utf8')  
        cursor = conn.cursor()
    #    cursor.execute('SET NAMES utf8;')  
    #    cursor.execute('SET character_set_connection=utf8;')  
    except Exception:
        logger.error('Faild to connect DBServer!', exc_info=True)
        return (None, None)
    return (cursor,conn)


def only_fetchall(cursor, conn, sql):
#    (cursor, conn) = connect_mssql(db_info)
    try:
        logger.debug(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        des = cursor.description
        logger.debug('...execute sql successfull!')
#     except Exception:
#         conn.rollback()
#         logger.error('...have problem, already rollback!', exc_info=True)
# #        print(e)
#         return None
    except Exception as e:
        conn.rollback()
        logger.error('...have problem, already rollback!', exc_info=True)
        print("error_com:", type(e),e)
        msg = str(e)
        return None,msg
    return res,des


def fetchall_sql(db_info, sql):
#    (cursor, conn) = connect_mssql()
    server = db_info[0]
    user = db_info[1]
    password = db_info[2]
    database = db_info[3]
    try:
        conn = pymssql.connect(server, user, password, database, login_timeout=5)
        cursor = conn.cursor()
    except Exception as e:
        logger.error('Faild to connect DBServer!', exc_info=True)
#        print(e)
        return None
    try:
        logger.debug(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        des = cursor.description
        logger.debug('...execute sql successfull!')
#        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        logger.error('...have problem, already rollback!', exc_info=True)
#        print(e)
        return None
    conn.close()
    return res,des


def execute_sql(db_info, sql):
#    (cursor, conn) = connect_mssql()
    server = db_info[0]
    user = db_info[1]
    password = db_info[2]
    database = db_info[3]
    try:
        conn = pymssql.connect(server, user, password, database, login_timeout=5)
        cursor = conn.cursor()
    except Exception as e:
        logger.error('Faild to connect DBServer!', exc_info=True)
#        print(e)
        return None
    try:
        logger.debug(sql)
        cursor.execute(sql)
        #res = cursor.fetchall()
        #des = cursor.description
        logger.debug('...execute sql successfull!')
#        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        logger.error('...have problem, already rollback!', exc_info=True)
#        print(e)
        return None
    conn.close()
    return None


'''
Get the table data and titlename 
'''
def get_db_data(sql, db_info):
    (cursor, conn) = connect_mssql(db_info)
    try:
#        print(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        des = cursor.description
        title = list(zip(*des))[0]
        print('...execute successfull!')
    except Exception as e:
        conn.rollback()
        print('...have problem, already rollback!')
        print(e)
    conn.close()
    return res,title


#return pandas
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