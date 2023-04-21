# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 22:56:21 2021

@author: Guest_1
"""

import cx_Oracle
import pandas as pd
import os
import numpy as np
import datetime
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt

from support_tool.Connection import df_to_oratb as dto_spt
from support_tool.Connection import Connect_y4abii as connection
  

done_table = ['E01_ORI_CLIP_FULLOG', 'E01_ORI_PROMOTION_T1_FULLOG',
              'E01_ORI_PROMOTION_T2_FULLOG','E01_ORI_CSF_T1_FULLOG', 
              'E01_ORI_CSF_T2_FULLOG','E01_ORI_CSF_T3_FULLOG',
              'E01_ORI_REDEMTION_FULLOG']
table_list = []

for table in table_list:
    print(f'{datetime.datetime.now()} {table} - start migrating' )
    ora_table = table
    excel_file = table
    excel_path = r'D:\Download Management\Avc_CoopInvoices\Database\TRANSIT_TABLE'
    exclusive_cols = ['ID']
    os.chdir(excel_path)
    df_input = pd.read_excel(excel_file + '.xlsx')

    dto_spt.df_to_oratable(df_input, exclusive_cols, ora_table, True)
    print(f'{datetime.datetime.now()} {table} - Complete migrating' )
    
    
    