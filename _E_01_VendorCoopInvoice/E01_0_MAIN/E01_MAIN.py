# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 16:25:08 2021

@author: NGUYEN Y
"""



import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')

task_id = 11
from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn 


from support_tool import support_tool as spt

from __Z_MAIN.Z_01_TASK_MANAGEMENT import Z01_spt as o_spt
# from _A_01_Retail_PO.A01_MAIN import A01_MAIN_spt as s_spt

from subprocess import *
import subprocess as sp

import datetime

import os
import pandas as pd
import pytz

from time import sleep

pt = pytz.timezone('US/Pacific')
vn = pytz.timezone('Asia/Ho_Chi_Minh')
check_firsttime = True
task_content = query.query(biicnn, """ select CONTENT from SYSTEM.AA_TASK_LIST where id = """ +str(task_id)).iloc[0]['CONTENT']
print(f'\n--------------------------------{task_content}------------------------------------------')

while True:
    if check_firsttime == False:
        #print('RUN THE 1ST BATCH')
        # RERVED TASK
        task_content = query.query(biicnn, """ select CONTENT from SYSTEM.AA_TASK_LIST where id = """ +str(task_id)).iloc[0]['CONTENT']
        status, task_info = o_spt.reserved_task(task_id)
        if status == False:
            print()
            print('CHECK ERROR: CANNOT RESERVED THE TASK')
            break
        
        
        # KICK OFF TO START PROCESS
        status = o_spt.kick_off_task(task_info)
        if status == False:
            print()
            print('CHECK ERROR: CANNOT KICK OFF PROCESS THE TASK')
            break
    count_error = o_spt.count_err(task_id)
    if count_error >0:
        print()
        print('ERROR HAPPENING: PLEASE CHECK BEFORE CONTINUE RUNING')
        break
    #######################################################################################
    # RUN TASK
    print(f'\n--------------------------------{task_content}------------------------------------------')
    
    print(f'\t--\t-- [{datetime.datetime.now()}] --')
    print(f'\t--\t--\t-- GEN REQ OVR-- ')
    sub_link = r'D:\Vendor_data_collect\_E_01_VendorCoopInvoice\E01_1_Overall\E01_1_Genreq.py'
    run_task = sp.run([sub_link, "ArcView"], shell=True, stdin=PIPE, stdout=PIPE, stderr = PIPE)
    count_error = o_spt.count_err(task_id)
    if count_error >0:
        print()
        print('ERROR HAPPENING: PLEASE CHECK BEFORE CONTINUE RUNING')
        break
    
    print(f'\t--\t-- [{datetime.datetime.now()}] --')
    print(f'\t--\t--\t-- RUN COLLECT OVERALL FILES -- ')
    sub_link = r'D:\Vendor_data_collect\_E_01_VendorCoopInvoice\E01_1_Overall\E01_1_Runreq.py'
    run_task = sp.run([sub_link, "ArcView"], shell=True, stdin=PIPE, stdout=PIPE, stderr = PIPE)
    count_error = o_spt.count_err(task_id)
    if count_error >0:
        print()
        print('ERROR HAPPENING: PLEASE CHECK BEFORE CONTINUE RUNING')
        break 

    ########################################
    print(f'\t--\t-- [{datetime.datetime.now()}] --')
    print(f'\t--\t--\t-- ASSIGN FOR DETAIL COLLECT-- ')
    sub_link = r'D:\Vendor_data_collect\_E_01_VendorCoopInvoice\E01_2_Detail\E01_2_Assignment.py'
    run_task = sp.run([sub_link, "ArcView"], shell=True, stdin=PIPE, stdout=PIPE, stderr = PIPE)
    count_error = o_spt.count_err(task_id)
    if count_error >0:
        print()
        print('ERROR HAPPENING: PLEASE CHECK BEFORE CONTINUE RUNING')
        break
    
    print(f'\t--\t-- [{datetime.datetime.now()}] --')
    print(f'\t--\t--\t-- RUN COLLECT DETAIL -- ')

    from _E_01_VendorCoopInvoice.E01_2_Detail import  E01_2_Runreq as spt
    spt.E02_collect_deatil()
    if count_error >0:
        print()
        print('ERROR HAPPENING: PLEASE CHECK BEFORE CONTINUE RUNING')
        break
    print('Done')
    ########################################
    
    
    if check_firsttime == False:    
        # CLOSE TASK
        status = o_spt.close_task(task_info)
        if status == False:
            print()
            print('CHECK ERROR: CANNOT CLOSE THE TASK')
            break    
    else:
        check_firsttime = False
    