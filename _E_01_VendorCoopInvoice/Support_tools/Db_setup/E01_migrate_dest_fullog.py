# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 09:02:06 2021

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


db_info = o_spt.db_info()
cnn, cursor = connection.connection()
tranman_table = pd.read_sql_query('select * from ' + db_info['DET']['trantb_man'],cnn)  
tranman_cols = pd.read_sql_query('select * from ' + db_info['DET']['trancol_man'],cnn)  

for index, row in tranman_table.iterrows():
    ori_table = row['TRANS_TABLE']
    dest_table = row['DEST_TABLE']
    transit_requirement = row['TRANSIT_REQUIREMENT']
    type_code = row['TYPE_CODE']
    
    if transit_requirement  == 1:
        print(f'Type [{type_code}] : from table [{ori_table}] to dest table [{dest_table}]' )
        tranman_cols_spec = tranman_cols[tranman_cols['TYPE_CODE']==type_code]
        
        for index_2, row_2 in tranman_cols_spec.iterrows():
            ori_cols = ','.join(tranman_cols_spec['TRANS_COL'])
            dest_cols = ','.join(tranman_cols_spec['DEST_COL'])
    
        
        # RUN SQL INPORT
        if announce == True:
            print(f'{datetime.datetime.now()} - Import table {dest_table}')
        sql_insert = 'insert into ' + dest_table + ' (REF_TYPE, ' + dest_cols + ') select ' + "'" + type_code + "', " + ori_cols  + ' from ' + ori_table
        cnn, cursor = connection.connection()
        cursor.execute(sql_insert)
        cnn.commit()
        cnn = None
        if announce == True:
            print(f'{datetime.datetime.now()} - Done')
        list_data = None
        sql_insert = None
        
        done_list = done_list + [[type_code, ori_table, dest_table]]