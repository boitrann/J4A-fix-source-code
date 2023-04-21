# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 11:40:48 2021

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

# Reset index 
db_info = o_spt.db_info()
ora_table = db_info['DET']['req']

sql_alter = 'alter table ' + ora_table + ' modify req_id generated always as identity restart start with 35'
cnn, cursor = connection.connection()
cursor.execute(sql_alter)
cnn.commit()
cnn = None
sql_alter = None
ora_table = None