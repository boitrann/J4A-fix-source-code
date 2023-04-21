# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 15:03:18 2021

@author: Guest_1
"""

import sys
sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID' : 'E01_1_1',
           'MODULE_CONTENT': 'COOPINV: GEN REQ CL'}

er_info['PHASE_NOTE'] = '1'
#from time import sleep
try:
    from support_tool.Connection import query as query
    from support_tool.Connection import Connect_y4abii as biicnn 
    from time import sleep
except:
    e = sys.exc_info()
    print(f'{e[0]} - ')
    print(f'{e[1]} - ')
    print(f'{e[2]} - ')
    sleep(20)

try:
    #import pandas as pd
    #import os
    er_info['PHASE_NOTE'] = '2'
    import datetime
    from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
    import pytz
    
    pt = pytz.timezone('US/Pacific')
    utc = pytz.utc 
    
    
    wide_scan_weekday = 'Saturday'
    
    # Nối DB.Table_name
    db_if = o_spt.db_info_new()
    
    consider_time = datetime.datetime.now(tz = pt).replace(tzinfo=None) #2023-04-20 20:23:01.984147(-07:00) -> None 
    consider_weeknum = consider_time.strftime('%A') #Thursday
    
    req_info = {}
    
    er_info['PHASE_NOTE'] = '3'
    req_info['REQ_TIME'] = consider_time.strftime('%Y-%m-%d %H:%M:%S')
    
    if consider_weeknum == wide_scan_weekday:
        period_len = 90
    else:
        period_len = 60
    
    prior_days = 0 #???
    
    er_info['PHASE_NOTE'] = '4'
    consider_time_filter = consider_time -  datetime.timedelta(days = prior_days)   
     
    req_info['STATUS'] = 'Open'
    req_info['FROM_DATE'] = consider_time_filter -  datetime.timedelta(days = period_len - 1)
    req_info['FROM_DATE'] =  datetime.date(req_info['FROM_DATE'].year,req_info['FROM_DATE'].month, req_info['FROM_DATE'].day) #%Y-%m-%d

    req_info['TO_DATE'] = datetime.date(consider_time_filter.year, consider_time_filter.month, consider_time_filter.day)

    req_info['FROM_DATE'] =  req_info['FROM_DATE'].strftime('%Y-%m-%d %H:%M:%S')
    req_info['TO_DATE'] =  req_info['TO_DATE'].strftime('%Y-%m-%d %H:%M:%S')
    
    print('New request ds order info: ')
    for key, value in req_info.items():
        print(f'\t\t-- {key} : {value}')
    print(f'\n----------------------------------------------------------------------------------------------------')
    
    er_info['PHASE_NOTE'] = '5'
    # Update New Rquest into db:
    # INSERT TABLE system.E01_COIN_REQ_OVERALL (output_cols) values (to_date(:1,'YYYY-MM-DD HH24:MI:SS),to_date(:2,'YYYY-MM-DD HH24:MI:SS),...)
    query.json_to_db(biicnn, req_info, ['REQ_ID'], db_if['ovr_rq']['db'], db_if['ovr_rq']['tb'], False)
    

except:
    e = sys.exc_info()
    er_info['LOG_TIME'] = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    er_info['ER1'] = e[0]
    er_info['ER2'] = e[1]
    er_info['ER3'] = e[2]
    er_info['STATUS'] = 'NEW'
    # INSERT TABLE AA_ERROR_LOG (output_cols) values (to_date(:1,'YYYY-MM-DD HH24:MI:SS),to_date(:2,'YYYY-MM-DD HH24:MI:SS),...)
    query.json_to_oratable(biicnn, er_info, ['LOG_ID'], 'AA_ERROR_LOG', False)        
            
        
        
        
        
