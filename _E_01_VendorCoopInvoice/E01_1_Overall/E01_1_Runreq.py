# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 15:03:31 2021

@author: Guest_1
"""

import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')

import sys
sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID' : 'E01_1_2',
           'MODULE_CONTENT': 'COOPINV: RUN CL OVR'}

er_info['PHASE_NOTE'] = '1'
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
    er_info['PHASE_NOTE'] = '2'
    import datetime

    import pandas as pd
    import numpy as np
    import os

    from support_tool import support_tool as spt
    from _E_01_VendorCoopInvoice.E01_1_Overall import E01_1_Spt as s_spt
    from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
    from support_tool.Connection import df_to_oratb as dto_spt
    from _E_01_VendorCoopInvoice.Support_tools import E01_db_update as db_upd
    from __Z_MAIN.Z_01_TASK_MANAGEMENT import Z01_spt as main_spt

    from support_tool.Connection import query as query
    from support_tool.Connection import Connect_y4abii as biicnn


    from login import openbrowser
    from login import login
    from login_page import  login_amz_vendor_central as lg
    import pytz

    er_info['PHASE_NOTE'] = '3'
    pt = pytz.timezone('US/Pacific')


    db_if = o_spt.db_info_new()
    fd_cf = o_spt.folder_config()

    er_info['PHASE_NOTE'] = '4'
    # Config folder
    path_ori = fd_cf['collected_path_ori']
    path_conv = fd_cf['collected_path_conv']
    path_pdf = fd_cf['pdf_path']
    #database_path = r'D:\Download Management\_A02_Retail Sales Invoices\Database'


    # Scan request:
    # Scan request:
    er_info['PHASE_NOTE'] = '5'
    ready_to_run, req_info =  s_spt.scan_collectdetail_req()
    # use table system.E01_COIN_REQ_OVERALL
    # => ready_to_run = True, req_info = {'REQ_ID':..., 'FROM_DATE':..., 'TO_DATE':...}

    if ready_to_run == True:
        er_info['PHASE_NOTE'] = '6'
        # Update request status
        req_info['STATUS'] = 'On_process'
        req_info['START_TIME'] = spt.getnow(pt)
        query.json_update_to_db(biicnn, req_info, ['STATUS', 'START_TIME'], ['REQ_ID'],
                                db_if['ovr_rq']['db'], db_if['ovr_rq']['tb'])
        """
        UPDATE SYSTEM.E01_COIN_REQ_OVERALL
        SET status = req_info['STATUS'], start_time = ['START_TIME']
        WHERE req_id = req_info['REQ_ID']
        """

        er_info['PHASE_NOTE'] = '7'
        # Break date range into small batch
        max_duration = 15
        filter_date_list = s_spt.break_date_duration(req_info['FROM_DATE'], req_info['TO_DATE'], max_duration)
        #filter_date_list = retinv_spt.break_date_duration(tg_start_date_str, tg_end_date_str, max_duration)
        filter_conditions = []

        er_info['PHASE_NOTE'] = '8'
        for i in range(len(filter_date_list)):
            filter_conditions.append({
                                        'Start_date': filter_date_list[i]['Start_date'],
                                        'End_date': filter_date_list[i]['End_date'],
                                        'Download_Status': False
                                     })

        er_info['PHASE_NOTE'] = '9'
        # Reserve bot:
        bot_profile , assign_status = main_spt.assign_bot_center(biicnn, er_info['MODULE_ID'], req_info['REQ_ID'])
        chrome_profile_folder = bot_profile['PROFILE_PATH']
        temp_dl_path = bot_profile['DOWNLOAD_PATH']

        er_info['PHASE_NOTE'] = '10'
        driver =lg.login_with_cookies(temp_dl_path)
        try:
            driver = lg.check_logout(driver)
        except:
            pass
        # driver = openbrowser.open_chrome( bot_profile["PROFILE_PATH"], 'D:/Chrome_driver/chromedriver.exe')
        # login.amz_vendor_login(driver)

        er_info['PHASE_NOTE'] = '11'
        # Get Summary_files
        summary_files_info,er_info = s_spt.collect_summaries(driver,temp_dl_path, filter_conditions, req_info,er_info)

        er_info['PHASE_NOTE'] = '12'
        # Release bot:
        main_spt.clean_chrome_history(driver)
        driver.quit()

        er_info['PHASE_NOTE'] = '13'
        # Realease bot status
        query.run_sql(biicnn, """update SYSTEM.BOT_CENTER_MANAGEMENT 
                                   set status = 'Available', 
                                   task = Null,
                                   req_id = Null,
                                   update_time_vn = to_date('""" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                          + """','YYYY-MM-DD HH24:MI:SS')
                                   where id = """ + str(bot_profile['ID']))

        er_info['PHASE_NOTE'] = '14'
        # Combine Summary File:
        df_sum_new, converted_file_name = s_spt.combine_summary_file(summary_files_info, path_conv, req_info)


        er_info['PHASE_NOTE'] = '15'
        #Process new summary: Fullog & Master
        df_sum_new, req_note = s_spt.process_new_sum(df_sum_new)

        er_info['PHASE_NOTE'] = '16'
        # Delete duplicate req id in fullog:
        query.run_sql(biicnn, 'delete from ' + db_if['ovr_fl']['full'] +
                              ' where req_id = ' + str(req_info['REQ_ID']))

        #Update fullog
        query.df_to_db(biicnn, df_sum_new, [], db_if['ovr_fl']['db'], db_if['ovr_fl']['tb'], True)

        er_info['PHASE_NOTE'] = '17'
        #Update master
        s_spt.update_fullog_to_master(db_if['ovr_mt']['db'], db_if['ovr_mt']['tb'],
                                      db_if['ovr_fl']['db'], db_if['ovr_fl']['tb'],
                                      req_info,'INVOICE_ID', ['TYPE_INFO', 'FEE_ID'], True )

        er_info['PHASE_NOTE'] = '18'
        count = 0
        for item in summary_files_info:
            count = count +1
            if count == 1:
                collected_files_text = '[' +  item['File_name'] +']'
            else:
                collected_files_text = collected_files_text + ', [' +  item['File_name'] +']'

        er_info['PHASE_NOTE'] = '19'
        # Update request collect detail:
        s_spt.gen_request_detail(int(req_info['REQ_ID']))

        er_info['PHASE_NOTE'] = '20'
        # Update request status
        req_info['STATUS'] = 'Complete'
        req_info['FINISH_TIME'] = spt.getnow(pt)
        req_info['COLLECTED_FILE_NAME'] = collected_files_text
        req_info['CONVERTED_FILE_NAME'] = converted_file_name
        req_info['REMARKS'] = req_note
        query.json_update_to_db(biicnn, req_info, ['STATUS', 'FINISH_TIME',
                                                   'COLLECTED_FILE_NAME', 'CONVERTED_FILE_NAME',
                                                   'REMARKS'], ['REQ_ID'],
                                                    db_if['ovr_rq']['db'], db_if['ovr_rq']['tb'])
        

except:
    e = sys.exc_info()
    print(e)
    er_info['LOG_TIME'] = datetime.datetime.now()
    er_info['ER1'] = e[0]
    er_info['ER2'] = e[1]
    er_info['ER3'] = e[2]
    er_info['STATUS'] = 'NEW'

    query.json_to_db(biicnn, er_info, ['LOG_ID'], 'SYSTEM','AA_ERROR_LOG', False)
            
                