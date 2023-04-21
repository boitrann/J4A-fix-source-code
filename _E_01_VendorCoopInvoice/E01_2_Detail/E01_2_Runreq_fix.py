# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 10:11:01 2021

@author: Guest_1
"""

import sys

sys.path.insert(
    0, 'D:\Vendor_data_collect')
import sys

sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID': 'E01_3_2',
           'MODULE_CONTENT': 'RUN CL DETAIL'}

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

cancel_condition = True

try:
    er_info['PHASE_NOTE'] = '2'
    import pandas as pd

    import datetime

    from support_tool import support_tool as spt
    from _E_01_VendorCoopInvoice.E01_2_Detail import E01_2_Spt as s_spt
    from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
    from __Z_MAIN.Z_01_TASK_MANAGEMENT import Z01_spt as main_spt

    from login import openbrowser
    from login import login

    import pytz

    pt = pytz.timezone('US/Pacific')

    db_if = o_spt.db_info_new()
    fd_cf = o_spt.folder_config()

    er_info['PHASE_NOTE'] = '3'
    # Scan request:
    print(
        f'\n----------------------------------------------------------------------------------------------------\n--[{datetime.datetime.now()}]--')
    print('\t-- Scan Request')
    df_req, ready_to_run, req_info = s_spt.scan_req_wipurpose('Run_req')

    for key, value in req_info.items():
        print(f'{key} - {value}')

    er_info['PHASE_NOTE'] = '4'

    if ready_to_run == True:
        er_info['PHASE_NOTE'] = '5'
        req_id = req_info['REQ_ID']
        # Update Status of request:
        req_info['STATUS'] = 'On_process'
        req_info['START_TIME'] = spt.getnow(pt)
        query.json_update_to_db(biicnn, req_info, ['STATUS', 'START_TIME'], ['REQ_ID'],
                                db_if['dt_rq']['db'], db_if['dt_rq']['tb'])

        er_info['PHASE_NOTE'] = '7'
        # Reserve bot:

        er_info['PHASE_NOTE'] = '6'
        # EACH BOT PROCESS
        # Allocate batch to bot

        er_info['PHASE_NOTE'] = '8'

        ######
        # driver = openbrowser.open_chrome( bot_profile["PROFILE_PATH"], 'D:/Chrome_driver/chromedriver.exe')

        er_info['PHASE_NOTE'] = '9'
        count_try = 0

        er_info['PHASE_NOTE'] = '22'
        # COMBINE DETAIL
        # Scan detail
        er_info['PHASE_NOTE'] = '23'
        df_req_detail = s_spt.get_req_detail_list(req_id, '')
        status_type = 'STATUS_DETAIL'
        # process by invoice number
        for index_1, rq_row in df_req_detail.iterrows():
            status_detail = rq_row[status_type]
            er_info['PHASE_NOTE'] = '24'
            if status_detail == 'Collected':
                er_info['PHASE_NOTE'] = '25'
                invoice_number = rq_row['INVOICE_NUMBER']

                df_file_name = s_spt.get_list_file_by_invoicenumber(req_id, invoice_number)
                # process by file
                for index, row in df_file_name.iterrows():
                    er_info['PHASE_NOTE'] = '26'
                    file_name = row['FILE_NAME']
                    withdraw_time = row['WITHDRAW_TIME']

                    er_info['PHASE_NOTE'] = '27'
                    # thêm data vào các bảng trong trans_table
                    # update vào e01_detail_fileman
                    not_def_check, remark_text, type_match_text = s_spt.process_detail_file(req_id, file_name,
                                                                                            invoice_number,
                                                                                            withdraw_time)

                    er_info['PHASE_NOTE'] = '28'
                    s_spt.update_file_type_info_to_db(req_id, invoice_number, file_name, not_def_check, remark_text,
                                                      type_match_text)

                er_info['PHASE_NOTE'] = '29'
                s_spt.update_req_detail_list_status(req_id, invoice_number, status_type)
                print(f'{datetime.datetime.now()} - Done process {index_1 + 1} for inv num: {invoice_number}')

        er_info['PHASE_NOTE'] = '30'
        # Update dest table fullog:
        done_list_1 = s_spt.update_dest_fullog(req_id, True)

        er_info['PHASE_NOTE'] = '31'
        # Update dest table master
        done_list_2 = s_spt.update_dest_master(req_id)

        er_info['PHASE_NOTE'] = '32'
        # crosscheck data:
        s_spt.run_table_validation()

        er_info['PHASE_NOTE'] = '33'
        # Update Status of request:
        req_info['STATUS'] = 'Complete'
        req_info['FINISH_TIME'] = spt.getnow(pt)
        query.json_update_to_db(biicnn, req_info, ['STATUS', 'FINISH_TIME'], ['REQ_ID'],
                                db_if['dt_rq']['db'], db_if['dt_rq']['tb'])

except:
    e = sys.exc_info()
    er_info['LOG_TIME'] = datetime.datetime.now(tz=pt).replace(tzinfo=None)
    er_info['ER1'] = e[0]
    er_info['ER2'] = e[1]
    er_info['ER3'] = e[2]
    er_info['STATUS'] = 'NEW'

    query.json_to_oratable(biicnn, er_info, ['LOG_ID'], 'AA_ERROR_LOG', False)




