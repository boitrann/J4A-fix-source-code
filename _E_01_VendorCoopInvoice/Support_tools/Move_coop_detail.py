# -*- coding: utf-8 -*-
"""
Created on Tue Sep  7 08:33:25 2021

@author: NGUYEN Y
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
import os
import shutil
import numpy as np


ori_path = r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\SHORTAGE'

target_path =  r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\DETAIL'

df_files = query.query(biicnn, 
                       """select * from E01_COIN_DETAIL_FILEMAN
                       where req_id >=35"""
                       )
df_files['Avai_Status'] = np.nan
df_files['Move_Status'] = np.nan
count_avai = 0
total = len(df_files)
count_move = 0

for index, row in df_files.iterrows():
    avai_status = False
    move_status = False
    req_id = row['REQ_ID']
    detail_file_name = row['FILE_NAME']
    avai_status = os.path.isfile( os.path.join(ori_path + '\\' + 'REQ_' + str(req_id), detail_file_name) ) 
    if avai_status == True:
        try: 
            os.mkdir(target_path + '\\' + 'REQ_' + str(req_id)) 
        except:
            pass
        shutil.move(os.path.join(ori_path + '\\' + 'REQ_' + str(req_id), detail_file_name), 
                    os.path.join(target_path + '\\' + 'REQ_' + str(req_id), detail_file_name))
        
        
        
        move_status = True
        count_move = count_move + 1
    avai_status = os.path.isfile( os.path.join(target_path + '\\' + 'REQ_' + str(req_id), detail_file_name) ) 
        #break
    
    df_files.loc[df_files[df_files['ID']==row['ID']].index, 'Avai_Status'] = avai_status
    df_files.loc[df_files[df_files['ID']==row['ID']].index, 'Move_Status'] = move_status
    if avai_status:
        count_avai = count_avai + 1
        
    print(f'\r{index+1} - move: {count_move} - Avai: {count_avai} - Total: {total}    ', end = '\r')
    