# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 10:32:19 2021

@author: Guest_1
"""

import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')
import sys
sys.path.append('D:\Vendor_data_collect')


from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn 

import pandas as pd
import datetime
def drop_table(ora_table):
   cnn, cursor = biicnn.connection()
   cursor.execute('drop table ' + ora_table)
   cnn.commit()
    

def reset_index_que(ora_table, id_col, reset_from):
   # Reset index
   sql_alter = 'alter table ' + ora_table + ' modify ' + id_col + ' generated always as identity restart start with ' + str(reset_from)
   cnn, cursor = biicnn.connection()
   cursor.execute(sql_alter)
   cnn.commit()
            
def update_fullog_to_master(table, req_info,id_column,ignore_columns,announce):
    master_table = table
    fullog_table = table.replace('_MASTER','_FULLOG')
    
    # select data type from ora table
    df_cols_mt = query.query(biicnn, """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                          where table_name = '""" + master_table + "'")
    df_cols_fl = query.query(biicnn, """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                          where table_name = '""" + fullog_table + "'")
    
    transf_cols = []
    for col_mt in df_cols_mt['COLUMN_NAME']:
        
        for col_fl in df_cols_fl['COLUMN_NAME']:
            if col_mt not in ignore_columns and col_mt == col_fl:
                transf_cols = transf_cols + [col_mt]
    
    transf_cols_text = ','.join(transf_cols)
    
    # Delete the same invoice id in master
    query.run_sql(biicnn, "delete from " + master_table + 
                          " where " + id_column  + """ in (select distinct(""" + id_column + """) 
                                                            from """ + fullog_table + 
                                                            """ where UPDATE_STATUS = 'Update' 
                                                                and REQ_ID = """ + str(req_info['REQ_ID']) + ")")
   
    
    
    # RUN SQL IMPORT
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {master_table}')
    sql_insert =( 'INSERT INTO ' + master_table + ' (' + transf_cols_text + 
                ') select ' + transf_cols_text + ' from ' + fullog_table + ' where req_id = ' + str(req_info['REQ_ID']) 
                + " and UPDATE_STATUS <> 'Duplicated'")
    query.run_sql(biicnn, sql_insert)
    
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')


# reset_index_que('SYSTEM.bot_center_management','ID',84)