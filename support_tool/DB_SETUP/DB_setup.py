# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 05:05:23 2021

@author: Guest_1
"""


from support_tool.Connection import query as query
import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')
    
def BOT_CENTER_MANAGEMENT():
    ora_table = 'BOT_CENTER_MANAGEMENT'   
    from support_tool.Connection import Connect_y4abii as connection
    
    try:
        query.run_sql(connection, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        BOT_CODE varchar2(255),
                        COMPUTER_NAME VARCHAR2(255),
                        USER_NAME VARCHAR2(255),
                        DOWNLOAD_PATH varchar2(2000), 
                        PROFILE_PATH varchar2(2000), 
                        UPDATE_TIME_VN timestamp,                        
                        STATUS varchar2(255), 
                        TASK varchar2(1000), 
                        REQ_ID number(30),
                        LAST_RESPON_TIME TIMESTAMP,
                        PRIMARY KEY(ID)) """
    
    query.run_sql(connection, sql)


 
def BOT_LIMIT_BY_COMPUTER():
    ora_table = 'BOT_LIMIT_BY_COMPUTER'   
    from support_tool.Connection import Connect_y4abii as connection
    
    try:
        query.run_sql(connection, 'drop table ' + ora_table)
    except:
        pass   
    
    sql = """CREATE TABLE """ + ora_table + """ (
                        COMPUTER_NAME VARCHAR2(255), 
                        LIMIT_ACCESS NUMBER(30),
                        REMARKS VARCHAR2(255),
                        PRIMARY KEY(COMPUTER_NAME)) """
    
    query.run_sql(connection, sql)
        