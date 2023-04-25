# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 10:10:43 2021

@author: Guest_1
"""

import sys

sys.path.insert(
    0, 'D:\Vendor_data_collect')

sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID': 'E01_2_1',
           'MODULE_CONTENT': 'COOPINV: ASSIGN DETAIL'}

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
    cancel_condition = True

    import datetime

    from _E_01_VendorCoopInvoice.E01_2_Detail import E01_2_Spt as s_spt
    from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt

    # from login import openbrowser
    # from login import login
    import numpy as np

    import math

    import pytz

    pt = pytz.timezone('US/Pacific')
    # utc = pytz.utc

    er_info['PHASE_NOTE'] = '3'
    # profile

    db_if = o_spt.db_info_new()
    fd_cf = o_spt.folder_config()

    # Gen new pending req if there is no request pending
    df_req_oa_pending = query.query(biicnn, "select * from " + db_if['dt_rq']['full'] +
                                    " where STATUS = 'Pending - Waiting for adding' ")
    # table = SYSTEM.E01_COIN_REQ_DETAIL
    current_pending = len(df_req_oa_pending)
    if current_pending == 0:
        new_req_info = {}
        new_req_info['REQ_TIME'] = datetime.datetime.now(tz=pt).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
        new_req_info['REQ_SOURCE'] = '[Re-scan]'
        new_req_info['STATUS'] = 'Pending - Waiting for adding'
        # insert new request
        query.json_to_db(biicnn, new_req_info, [], db_if['dt_rq']['db'], db_if['dt_rq']['tb'], False)

    er_info['PHASE_NOTE'] = '4'
    # Scan request:
    print(
        f'\n----------------------------------------------------------------------------------------------------\n--[{datetime.datetime.now()}]--')
    print('\t-- Scan Request')
    df_req, ready_to_run, req_info = s_spt.scan_req_wipurpose('Assignment')

    # if request ready to run:
    if ready_to_run == True:
        er_info['PHASE_NOTE'] = '5'
        # Add missing order id into current pending:
        s_spt.add_missing_detail_shortage(req_info)
        s_spt.add_missing_detail_promotion(req_info)
        s_spt.add_fail_and_other(req_info)

        er_info['PHASE_NOTE'] = '6'
        # Activate request to open
        req_info['STATUS'] = 'Open'
        query.json_update_to_db(biicnn, req_info, ['STATUS'], ['REQ_ID'], db_if['dt_rq']['db'], db_if['dt_rq']['tb'])

        # ASSIGNMENT
        er_info['PHASE_NOTE'] = '7'
        # Scan request detail:
        df_req_detail_ori = query.query(biicnn,
                                        'select * from ' + db_if['dt_rql']['full'] +
                                        " where REQ_ID = " + str(req_info['REQ_ID']))

        er_info['PHASE_NOTE'] = '8'
        # Define request per bot
        total_request = len(df_req_detail_ori)
        # num_bot = len(bot_info)
        num_batch = 1
        min_request_per_bot = max(math.ceil(total_request / num_batch), 100)
        acc_assign = 0
        assigned_batch_list = []
        df_req_detail_ori['ASSIGNED_BATCH'] = np.nan
        df_req_detail_ori = df_req_detail_ori.sort_values(by='ID', ascending=True)
        # Assignment
        er_info['PHASE_NOTE'] = '9'
        if len(df_req_detail_ori)>0:
            for batch in range(1, num_batch + 1): # Chạy 1 lần
                er_info['PHASE_NOTE'] = '10'
                print(batch)
                assign_list = []
                assign_list = df_req_detail_ori[((df_req_detail_ori['ASSIGNED_BATCH'].isnull()) &
                                                 (df_req_detail_ori['REQ_ID'] == req_info['REQ_ID']))].head(
                    min_request_per_bot)['ID']
                df_req_detail_ori.loc[
                    df_req_detail_ori[df_req_detail_ori['ID'].isin(assign_list)].index, 'ASSIGNED_BATCH'] = batch

                acc_assign = acc_assign + len(assign_list)
                assigned_batch_list = assigned_batch_list + [batch]

                min_id = df_req_detail_ori[df_req_detail_ori['ASSIGNED_BATCH'] == batch]['ID'].min()
                max_id = df_req_detail_ori[df_req_detail_ori['ASSIGNED_BATCH'] == batch]['ID'].max()

                er_info['PHASE_NOTE'] = '11'
                # Update list assigned bot into db:
                query.run_sql(biicnn, 'update ' + db_if['dt_rql']['full'] +
                              """ set ASSIGNED_BATCH = '""" + str(batch) + """' 
                              where ID >= """ + str(min_id) + ' and ID <= ' + str(max_id))

                if acc_assign == total_request:
                    break
        er_info['PHASE_NOTE'] = '12'
        # Update status of main request
        req_info['ASSIGNMENT_STATUS'] = 'Assigned'
        query.json_update_to_db(biicnn, req_info, ['ASSIGNMENT_STATUS'], ['REQ_ID'], db_if['dt_rq']['db'],
                                db_if['dt_rq']['tb'])



except:
    e = sys.exc_info()
    er_info['LOG_TIME'] = datetime.datetime.now(tz=pt).replace(tzinfo=None)
    er_info['ER1'] = e[0]
    er_info['ER2'] = e[1]
    er_info['ER3'] = e[2]
    er_info['STATUS'] = 'NEW'

    query.json_update_to_db(biicnn, er_info, ['LOG_ID'], 'SYSTEM', 'AA_ERROR_LOG', False)

