# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 09:58:06 2021

@author: Guest_1
"""



announce = True


import cx_Oracle
import pandas as pd
import os
import numpy as np
import datetime
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt

from support_tool.Connection import df_to_oratb as dto_spt
from support_tool.Connection import Connect_y4abii as connection
  
done_list = []
table_list = ['E01_COIN_CLIP_FULLOG', 'E01_COIN_PROMOTION_FULLOG', 'E01_COIN_CSF_FULLOG']
for table in table_list:
    fullog_table = table
    master_table = table.replace('_FULLOG', '_MASTER')
    #print( fullog_table + ' ' + master_table)
    # select data type from ora table
    sql_select = "SELECT  table_name, column_name, data_type FROM all_tab_columns where table_name = '" + fullog_table + "'"
    cnn, cursor = connection.connection()
    df_cols = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None
    transf_cols = []
    for col in df_cols['COLUMN_NAME']:
        transf_cols = transf_cols + [col]
    
    transf_cols_text = ','.join(transf_cols)
    
    
    # RUN SQL INPORT
    if announce == True:
        print(f'\t ---- {datetime.datetime.now()} - Import table {master_table}')
    sql_insert = 'insert into ' + master_table + ' (' + transf_cols_text + ') select ' + transf_cols_text + ' from ' + fullog_table
    cnn, cursor = connection.connection()
    cursor.execute(sql_insert)
    cnn.commit()
    cnn = None
    if announce == True:
        print(f'\t ---- {datetime.datetime.now()} - Done')
    list_data = None
    sql_insert = None
    
    done_list =done_list + [[fullog_table, master_table]]
    