# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 12:41:16 2020

@author: Guest_1
"""
import sys

sys.path.insert(
    0, 'D:\Vendor_data_collect')
import datetime
from time import sleep
from support_tool.Connection import Connect_y4abii as connection
import pandas as pd
import sqlalchemy as sqla
import cx_Oracle

try:
    cx_Oracle.init_oracle_client(lib_dir=r"D:\Download\instantclient_21_3")
except:
    pass


def connection():
    HOST = '172.30.12.144'
    PORT = '1521'
    SID = 'ORCDE1'
    dsn = cx_Oracle.makedsn(HOST, PORT, SID)
    connection = cx_Oracle.connect('user', 'pass', dsn)
    cursor = connection.cursor()
    return connection, cursor
    
def connection_international():
    HOST = '172.30.12.144'
    PORT = '1521'
    SID = 'y4abii'
    dsn = cx_Oracle.makedsn(HOST, PORT, SID)
    connection = cx_Oracle.connect('user', 'pass', dsn)
    cursor = connection.cursor()
    return connection, cursor


# a,b =connection()
def db_type():
    return 'oracle'


def lock_table_for_update(cnn, cursor, table_name):
    print(f'{datetime.datetime.now()} : Process Lock table for session')
    print()
    lock_status = False
    available_status = 'Null'

    sql_check_lock = "SELECT C.OWNER, C.OBJECT_NAME, C.OBJECT_TYPE, B.SID, B.SERIAL#,"
    sql_check_lock += "B.STATUS, B.OSUSER, B.MACHINE FROM V$LOCKED_OBJECT A, V$SESSION B,"
    sql_check_lock += "DBA_OBJECTS C WHERE B.SID = A.SESSION_ID AND A.OBJECT_ID = C.OBJECT_ID "
    sql_check_lock += "AND C.OBJECT_NAME = '" + table_name + "'"

    sql_lock = "select * from " + table_name + " for UPDATE"

    while lock_status == False:
        df_lock = pd.read_sql_query(sql_check_lock, cnn)
        if len(df_lock) == 0:
            cursor.execute(sql_lock)
        else:
            available_status = df_lock.iloc[0]['STATUS']
            if available_status == 'ACTIVE':
                lock_status = True
            else:
                sleep(0.5)
    print(f'\r -- {datetime.datetime.now()} : Availble Status {available_status} -- ', end='\r')

    return lock_status, df_lock


def get_full_table(ora_table):
    sql_select = 'select * from ' + ora_table
    cnn, cursor = connection.connection()
    df = pd.read_sql_query(sql_select, cnn)
    cnn = None
    sql_select = None

    return df