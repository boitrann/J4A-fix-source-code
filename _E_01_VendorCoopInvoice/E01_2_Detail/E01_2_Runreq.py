# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 10:11:01 2021

@author: Guest_1
"""

import sys

import pandas as pd
from support_tool import R0028_spt as sync
sys.path.insert(
	0, 'D:\Vendor_data_collect')
import sys
sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID' : 'E01_3_2',
           'MODULE_CONTENT': 'RUN CL DETAIL'}

er_info['PHASE_NOTE'] = '1'
try:
    from support_tool.Connection import query as query
    from support_tool.Connection import Connect_y4abii as biicnn
    from support_tool.Connection import Connect_y4a_int as bii_int
    from time import sleep
except:
    e = sys.exc_info()
    print(f'{e[0]} - ')
    print(f'{e[1]} - ')
    print(f'{e[2]} - ')
    sleep(20)

cancel_condition = True

def E02_collect_deatil():
    e = sys.exc_info()
    sleep(20)
    try:
        e = sys.exc_info()
        sleep(20)
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
        print(f'\n----------------------------------------------------------------------------------------------------\n--[{datetime.datetime.now()}]--')
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
            query.json_update_to_db(biicnn, req_info, ['STATUS','START_TIME'], ['REQ_ID'],
                                    db_if['dt_rq']['db'], db_if['dt_rq']['tb'])



            er_info['PHASE_NOTE'] = '7'
            # Reserve bot:
            bot_profile , assign_status = main_spt.assign_bot_center(biicnn, er_info['MODULE_ID'], req_info['REQ_ID'])
            temp_dl_path = bot_profile['DOWNLOAD_PATH']

            er_info['PHASE_NOTE'] = '6'
            # EACH BOT PROCESS
            # Allocate batch to bot
            bot_code = bot_profile['BOT_CODE']
            s_spt.bot_take_batch(bot_profile['BOT_CODE'], req_info)

            er_info['PHASE_NOTE'] = '8'

            ######
            from login_page import  login_amz_vendor_central as ls
            driver = ls.login_with_cookies(temp_dl_path)
            # driver = openbrowser.open_chrome( bot_profile["PROFILE_PATH"], 'D:/Chrome_driver/chromedriver.exe')



            er_info['PHASE_NOTE'] = '9'
            count_try = 0
            status_scc = False
            while status_scc == False:
                er_info['PHASE_NOTE'] = '10'
                count_try = count_try + 1
                # login.amz_vendor_login(driver)

                # try:
                    # Scan detail
                er_info['PHASE_NOTE'] = '11'
                #ora_table =
                # select table
                df_req_detail = query.query(biicnn, 'select * from ' + db_if['dt_rql']['full'] +
                                            " where REQ_ID = " + str(req_info['REQ_ID']) )

                none_collect_status = ['Collected', 'Complete', 'No_record']
                if len(df_req_detail[((~df_req_detail['STATUS_SHORTAGE'].isin(none_collect_status)) |
                                      (~df_req_detail['STATUS_DETAIL'].isin(none_collect_status)))])>0:
                    # start collect:
                    er_info['PHASE_NOTE'] = '12'
                    # collect
                    s_spt.download_shortage_detail_files(driver, df_req_detail, temp_dl_path, req_info['REQ_ID'])


                status_scc = True
                # except:
                #     pass


            if status_scc == False:
                status_scc.get()

            er_info['PHASE_NOTE'] = '13'
            # Release bot:
            main_spt.clean_chrome_history(driver)
            driver.quit()

            er_info['PHASE_NOTE'] = '14'
            # Realease bot status
            query.run_sql(biicnn, """update SYSTEM.BOT_CENTER_MANAGEMENT 
                                       set status = 'Available', 
                                       task = Null,
                                       req_id = Null,
                                       update_time_vn = to_date('""" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                              + """','YYYY-MM-DD HH24:MI:SS')
                                       where id = """ + str(bot_profile['ID']))


            er_info['PHASE_NOTE'] = '15'
            # COMBINE SHORTAGE:
            df_req_detail = s_spt.get_req_detail_list(req_id, '')
            status_type = 'STATUS_SHORTAGE'
            for index_1, rq_row in df_req_detail.iterrows():
                status_detail = rq_row[status_type]
                er_info['PHASE_NOTE'] = '16'
                if status_detail == 'Collected':
                    invoice_number = rq_row['INVOICE_NUMBER']

                    er_info['PHASE_NOTE'] = '17'

                    # Open file
                    df_new_shortage, withdraw_time = s_spt.open_shortage_file(req_id, invoice_number)
                    df_new_shortage['REQ_ID'] = req_id
                    df_new_shortage['WITHDRAW_TIME'] = withdraw_time
                    df_new_shortage['INVOICE_NUMBER'] = invoice_number

                    er_info['PHASE_NOTE'] = '18'
                    s_spt.update_transittb_df_to_fullog(req_id, df_new_shortage, invoice_number, db_if['st_fl']['tb'])
                    er_info['PHASE_NOTE'] = '19'
                    s_spt.update_req_detail_list_status(req_id, invoice_number, status_type)
                    print('Done ' + invoice_number)

            er_info['PHASE_NOTE'] = '20'
            # Update shortage master
            query.run_sql(biicnn,
                            """delete from """+ db_if['st_mt']['full']+"""
                            where invoice_number in (select invoice_number from """+ db_if['st_fl']['full']+"""
                                                         where req_id = """ + str(req_id) +')'
                                                     )
            er_info['PHASE_NOTE'] = '21'
            query.run_sql(biicnn,
                            """insert into """+ db_if['st_mt']['full']+"""
                            select * from """+ db_if['st_fl']['full']+"""
                            where req_id = """ + str(req_id) )

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
                        not_def_check, remark_text, type_match_text = s_spt.process_detail_file(req_id, file_name, invoice_number, withdraw_time)

                        er_info['PHASE_NOTE'] = '28'
                        s_spt.update_file_type_info_to_db(req_id, invoice_number, file_name, not_def_check, remark_text, type_match_text)

                    er_info['PHASE_NOTE'] = '29'
                    s_spt.update_req_detail_list_status(req_id, invoice_number, status_type)
                    print(f'{datetime.datetime.now()} - Done process {index_1 + 1} for inv num: {invoice_number}')

            er_info['PHASE_NOTE'] = '30'
            # Update dest table fullog:
            done_list_1 = s_spt.update_dest_fullog(req_id, True)

            er_info['PHASE_NOTE'] = '31'
            #Update dest table master
            done_list_2 = s_spt.update_dest_master(req_id)

            er_info['PHASE_NOTE'] = '32'
            # crosscheck data:
            s_spt.run_table_validation()

            tables = ['E01_COIN_CLIP_MASTER', 'E01_COIN_CSF_MASTER',
                      'E01_COIN_PROMOTION_MASTER', 'E01_COIN_SHORTAGE_MASTER', 'E01_ORI_REDEMTION_MASTER',
                      'E01_COIN_OVERALL_MASTER']
            for table in tables:
                sync.sync_table(table, 1)

            ### insert data 144
            tables_list = [['E01_COIN_PROMOTION_MASTER', 'Y4A_DWB_AMZ_COP_PRO'],
                           ['E01_COIN_CLIP_MASTER', 'Y4A_DWB_AMZ_COP_CLP'],
                           ['E01_COIN_CSF_MASTER', 'Y4A_DWB_AMZ_COP_CSF'],
                           ['E01_COIN_OVERALL_MASTER', 'Y4A_DWB_AMZ_COP_OVR'],
                           ['E01_COIN_SHORTAGE_MASTER', 'Y4A_DWB_AMZ_COP_STR'],
                           ['E01_ORI_REDEMTION_MASTER','Y4A_DWB_AMZ_COP_RDT']]
            for table in tables_list:
                ori_table = table[0]
                des_table = table[1]
                sql = f"select * from SYSTEM.{ori_table}"
                df = query.query(biicnn, sql)
                df['COUNTRY'] = 'USA'
                query.run_sql(bii_int, f"delete from  Y4A_INT.{des_table} where country ='USA'")
                query.df_to_db(bii_int, df, [], 'Y4A_INT', des_table, True)
            ## End note


            er_info['PHASE_NOTE'] = '33'
            # Update Status of request:
            req_info['STATUS'] = 'Complete'
            req_info['FINISH_TIME'] = spt.getnow(pt)
            query.json_update_to_db(biicnn, req_info, ['STATUS','FINISH_TIME'], ['REQ_ID'],
                                    db_if['dt_rq']['db'], db_if['dt_rq']['tb'])

    except:
        e = sys.exc_info()
        er_info['LOG_TIME'] = datetime.datetime.now(tz = pt).replace(tzinfo = None)
        er_info['ER1'] = e[0]
        er_info['ER2'] = e[1]
        er_info['ER3'] = e[2]
        er_info['STATUS'] = 'NEW'

        query.json_to_oratable(biicnn, er_info, ['LOG_ID'], 'SYSTEM.AA_ERROR_LOG', False)
# E02_collect_deatil()