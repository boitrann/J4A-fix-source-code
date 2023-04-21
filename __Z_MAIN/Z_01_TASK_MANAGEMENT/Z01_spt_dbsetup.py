# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 09:48:52 2021

@author: Guest_1
"""

import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')


from support_tool.Connection import query as query

def AA_TASK_LIST():
    ora_table = 'AA_TASK_LIST'   
    from support_tool.Connection import Connect_y4abii as biicnn
    
    try:
        query.run_sql(biicnn, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        CONTENT varchar2(255),
                        FREQUENCY NUMBER(30),
                        DAILY_STARTTIME_REQUIRED NUMBER(30),
                        DAILY_ENDTIME_REQUIRED NUMBER(30),
                        ACTIVE_STATUS varchar2(255),
                        REMARKS varchar2(255),
                        PRIMARY KEY(ID)) """
    print(sql)
    query.run_sql(biicnn, sql)
# AA_TASK_LIST()

def AA_TASK_DAILY_DETAIL():
    ora_table = 'AA_TASK_DAILY_DETAIL'   
    from support_tool.Connection import Connect_y4abii as biicnn
    
    try:
        query.run_sql(biicnn, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REVIEW_DATE timestamp,
                        TASK_ID NUMBER(30),
                        TIME_REQUIRED timestamp,
                        STATUS varchar2(255),
                        START_TIME timestamp,
                        FINISH_TIME timestamp,
                        DURATION_HOURS FLOAT(63),
                        REMARKS varchar2(2000),
                        PRIMARY KEY(ID)) """
    print(sql)
    query.run_sql(biicnn, sql)
        
# AA_TASK_DAILY_DETAIL()

def AA_TASK_DAILY_SUMMARY():
    ora_table = 'AA_TASK_DAILY_SUMMARY'   
    from support_tool.Connection import Connect_y4abii as biicnn
    
    try:
        query.run_sql(biicnn, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REVIEW_DATE timestamp,
                        TASK_ID NUMBER(30),
                        FREQUENCY_REQUIRED NUMBER(30),
                        KICK_OFF_COUNT NUMBER(30),
                        COMPLETE_COUNT NUMBER(30),
                        AVG_DURATION_HOURS FLOAT(63),
                        REMARKS varchar2(2000),
                        PRIMARY KEY(ID)) """
    query.run_sql(biicnn, sql)
        
        

def AA_ERROR_LOG():
    ora_table = 'AA_ERROR_LOG'   
    from support_tool.Connection import Connect_y4abii as biicnn
    
    try:
        query.run_sql(biicnn, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        LOG_ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        LOG_TIME timestamp,
                        MOTHER_TASK_ID NUMBER(30),
                        MOTHER_TASK_CONTENT varchar2(255),
                        MODULE_ID varchar2(255),
                        MODULE_CONTENT varchar2(255),
                        ER1 varchar2(255),
                        ER2 varchar2(255),
                        ER3 varchar2(255),
                        STATUS varchar2(255),
                        SOLVING_NOTE varchar2(2000),
                        REMARKS varchar2(2000),
                        PRIMARY KEY(LOG_ID)) """
    query.run_sql(biicnn, sql)



def BOT_MANAGEMENT_LOG():    
  
    from support_tool.Connection import Connect_y4abii as connection
    cnn, cursor = connection.connection()
    
    sql = """CREATE TABLE BOT_MANAGEMENT_LOG (
                        LOG_ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        ID NUMBER(30),
                        BOT_CODE varchar2(255),
                        COMPUTER_NAME varchar2(255),
                        USER_NAME varchar2(255),
                        DOWNLOAD_PATH varchar2(2000), 
                        PROFILE_PATH varchar2(2000), 
                        UPDATE_TIME_VN timestamp,
                        UPDATE_TIME_PT timestamp,                        
                        STATUS varchar2(255), 
                        TASK varchar2(1000), 
                        REQ_ID number(30),
                        PHASE_NOTE VARCHAR2(255),
                        
                        PRIMARY KEY(LOG_ID)) """
    
    cursor.execute(sql)
    
    connection = None

# AA_ERROR_LOG()
# AA_TASK_LIST()
# AA_TASK_DAILY_DETAIL()
# AA_TASK_DAILY_SUMMARY()
# BOT_MANAGEMENT_LOG()