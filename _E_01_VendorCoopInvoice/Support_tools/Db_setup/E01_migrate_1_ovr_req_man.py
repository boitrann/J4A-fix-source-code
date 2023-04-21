# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 11:15:07 2021

@author: Guest_1
"""


excel_file = '1_avccoopinv_batch_man'
excel_path = r'D:\Download Management\Avc_CoopInvoices\Database'


import cx_Oracle
import pandas as pd
import os
import numpy as np
import datetime
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt

from support_tool.Connection import df_to_oratb as dto_spt
from support_tool.Connection import Connect_y4abii as connection

db_info = o_spt.db_info()
ora_table = db_info['OVR']['req']


os.chdir(excel_path)
df_input = pd.read_excel(excel_file + '.xlsx')

dto_spt.df_to_oratable(df_input, ['REQ_ID'], ora_table, True)

