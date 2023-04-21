# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 10:11:12 2021

@author: Guest_1
"""

import sys

sys.path.append('D:\Vendor_data_collect')

import pandas as pd
import os
import datetime
from time import sleep

from support_tool import support_tool as spt
from _E_01_VendorCoopInvoice.E01_2_Detail import E01_2_Spt as s_spt
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt

# from login import openbrowser
# from login import login
import numpy as np

import math

import pytz

from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn


def scan_req_wipurpose(scan_purpose):
    db_if = o_spt.db_info_new()
    # fd_cf = o_spt.folder_config()

    # Scan request
    df_req = query.query(biicnn, 'select * from ' + db_if['dt_rq']['full'] + " where STATUS <> 'Complete'")

    # Check any on process
    on_process_count = len(df_req[((df_req['STATUS'] == 'On_process') & (df_req['ASSIGNMENT_STATUS'] == 'Assigned'))])
    if scan_purpose == 'Run_req':
        open_count = len(df_req[((df_req['STATUS'] == 'Open') & (df_req['ASSIGNMENT_STATUS'] == 'Assigned'))])
        req_id = df_req[((df_req['STATUS'] == 'Open') & (df_req['ASSIGNMENT_STATUS'] == 'Assigned'))]['REQ_ID'].min()
    elif scan_purpose == 'Assignment':
        current_open = len(df_req[df_req['STATUS'] == 'Open'])
        current_pending = len(df_req[df_req['STATUS'] == 'Pending - Waiting for adding'])
        if current_open == 0 and current_pending > 0:
            # active 1 current pending
            pending_id = df_req[df_req['STATUS'] == 'Pending - Waiting for adding']['REQ_ID'].min()
            df_req.loc[df_req[df_req['REQ_ID'] == pending_id].index, 'STATUS'] = 'Open'

        open_count = len(df_req[((df_req['STATUS'] == 'Open') & (df_req['ASSIGNMENT_STATUS'] != 'Assigned'))])
        req_id = df_req[((df_req['STATUS'] == 'Open') & (df_req['ASSIGNMENT_STATUS'] != 'Assigned'))]['REQ_ID'].min()
    req_info = {}

    ready_to_run = False
    if on_process_count > 0:
        print('Processing another request, please comeback later')
        # df_req_man = None
        ready_to_run = False
    elif open_count == 0:
        print('No Available request to process')
        # df_req_man = None
        ready_to_run = False
    elif req_id is np.nan:
        print('Please check, error in request ID = Null')
        # df_req_man = None
        ready_to_run = False
    else:
        ready_to_run = True
        req_info['REQ_ID'] = req_id
        req_info['REQ_SOURCE'] = df_req[df_req['REQ_ID'] == req_id].iloc[0]['REQ_SOURCE']

    return df_req, ready_to_run, req_info


def add_missing_detail_shortage(req_info):
    db_if = o_spt.db_info_new()

    df_new_req_detail = query.query(biicnn,
                                    """select distinct(invoice_id) as invoice_number from """ + db_if['ovr_mt'][
                                        'full'] + """
                            where invoice_id not in (select distinct(INVOICE_NUMBER) as INVOICE_NUMBER 
                                                     from """ + db_if['st_mt']['full'] + """) """)

    df_new_req_detail['REQ_ID'] = req_info['REQ_ID']
    df_new_req_detail['REQ_SOURCE'] = 'Add_missing'
    df_new_req_detail['STATUS_SHORTAGE'] = 'Open'
    df_new_req_detail['STATUS_DETAIL'] = 'Open'

    # bookded list
    df_booked_list = query.query(biicnn, 'select INVOICE_NUMBER from ' + db_if['dt_rql']['full'] +
                                 ' where REQ_ID = ' + str(req_info['REQ_ID']))

    df_new_req_detail = df_new_req_detail.drop(
        df_new_req_detail[df_new_req_detail['INVOICE_NUMBER'].isin(df_booked_list['INVOICE_NUMBER'])].index)

    # current_max_id = spt.get_max_id_ora(db_info['collectdetail']['req_tb_detail'], 'ID')
    # df_new_req_detail = df_new_req_detail.reset_index(drop = True)
    # df_new_req_detail['ID'] = current_max_id + df_new_req_detail.index + 1
    df_new_req_detail = df_new_req_detail[
        ['REQ_ID', 'REQ_SOURCE', 'INVOICE_NUMBER', 'STATUS_SHORTAGE', 'STATUS_DETAIL']]

    query.df_to_db(biicnn, df_new_req_detail, ['ID'], db_if['dt_rql']['db'], db_if['dt_rql']['tb'], True)


def add_missing_detail_promotion(req_info):
    db_if = o_spt.db_info_new()

    df_new_req_detail = query.query(biicnn,
                                    """select distinct(INVOICE_ID) as INVOICE_NUMBER 
                                    from """ + db_if['ovr_mt']['full'] + """ 
                                    where INVOICE_ID not in (select distinct(INVOICE_NUMBER) 
                                                             from """ + db_if['pr_mt']['full'] + """ )
                                    and type_info in ('NORMAL_PROMOTION_FUNDING_AGREEMENT','NORMAL_PROMOTION_FEE_AGREEMENT',
                                                         'COUPON_PROMOTION', 'PROMOTION') 
                                    and invoice_id not in (select invoice_number 
                                                           from """ + db_if['no_rec']['full'] + """)
                                    and invoice_id not in (select distinct(invoice_number) 
                                                           from """ + db_if['dt_fm']['full'] + """)"""
                                    )

    df_new_req_detail['REQ_ID'] = req_info['REQ_ID']
    df_new_req_detail['REQ_SOURCE'] = 'Add_missing_detail_promotion'
    df_new_req_detail['STATUS_SHORTAGE'] = 'Open'
    df_new_req_detail['STATUS_DETAIL'] = 'Open'

    # bookded list
    df_booked_list = query.query(biicnn, 'select INVOICE_NUMBER from ' + db_if['dt_rql']['full'] +
                                 ' where REQ_ID = ' + str(req_info['REQ_ID']))

    df_new_req_detail = df_new_req_detail.drop(
        df_new_req_detail[df_new_req_detail['INVOICE_NUMBER'].isin(df_booked_list['INVOICE_NUMBER'])].index)

    # current_max_id = spt.get_max_id_ora(db_info['collectdetail']['req_tb_detail'], 'ID')
    # df_new_req_detail = df_new_req_detail.reset_index(drop = True)
    # df_new_req_detail['ID'] = current_max_id + df_new_req_detail.index + 1
    df_new_req_detail = df_new_req_detail[
        ['REQ_ID', 'REQ_SOURCE', 'INVOICE_NUMBER', 'STATUS_SHORTAGE', 'STATUS_DETAIL']]

    query.df_to_db(biicnn, df_new_req_detail, ['ID'], db_if['dt_rql']['db'], db_if['dt_rql']['tb'], True)


def add_fail_and_other(req_info):
    db_if = o_spt.db_info_new()

    df_new_req_detail = query.query(biicnn,
                                    """select distinct(invoice_number) as INVOICE_NUMBER
                                        FROM (select * from """ + db_if['dt_rql']['full'] + """  
                                                where id in (select max(id) from """ + db_if['dt_rql']['full'] + """   
                                                            group by invoice_number)) tb1
                                        WHERE ( ( (remarks IS NULL OR remarks = '"No records were found."') 
                                                   and status_detail = 'No_record'
                                                ) 
                                                or  status_detail = 'Fail'
                                              )
                                             AND INVOICE_NUMBER IN (SELECT INVOICE_NUMBER 
                                                                    FROM """ + db_if['coop_vali']['full'] + """    
                                                                    WHERE DIFF_OVR_DET <>0)
                                    """
                                    )

    df_new_req_detail['REQ_ID'] = req_info['REQ_ID']
    df_new_req_detail['REQ_SOURCE'] = 'Add_last_fail_or_no_record_not_compliance'
    df_new_req_detail['STATUS_SHORTAGE'] = 'Open'
    df_new_req_detail['STATUS_DETAIL'] = 'Open'

    # bookded list
    df_booked_list = query.query(biicnn, 'select INVOICE_NUMBER from ' + db_if['dt_rql']['full'] +
                                 ' where REQ_ID = ' + str(req_info['REQ_ID']))

    df_new_req_detail = df_new_req_detail.drop(
        df_new_req_detail[df_new_req_detail['INVOICE_NUMBER'].isin(df_booked_list['INVOICE_NUMBER'])].index)

    # current_max_id = spt.get_max_id_ora(db_info['collectdetail']['req_tb_detail'], 'ID')
    # df_new_req_detail = df_new_req_detail.reset_index(drop = True)
    # df_new_req_detail['ID'] = current_max_id + df_new_req_detail.index + 1
    df_new_req_detail = df_new_req_detail[
        ['REQ_ID', 'REQ_SOURCE', 'INVOICE_NUMBER', 'STATUS_SHORTAGE', 'STATUS_DETAIL']]

    query.df_to_db(biicnn, df_new_req_detail, ['ID'], db_if['dt_rql']['db'], db_if['dt_rql']['tb'], True)


def bot_take_batch(bot_code, req_info):
    # db_info = o_spt.db_info()
    db_if = o_spt.db_info_new()

    id_columns = 'ASSIGNED_BATCH'
    # table_name = db_info['DET']['req_detail']
    # Get current max id from rquest man
    df = query.query(biicnn,
                     'select min(' + str(id_columns) + ')  as MIN from ' + db_if['dt_rql']['full'] +
                     " where REQ_ID = " + str(req_info['REQ_ID']) + " and ASSIGNED_BOT is Null")

    req_batch_id = df.iloc[0]['MIN']
    if req_batch_id is None:
        req_batch_id = 0
    df = None

    if req_batch_id > 0:
        query.run_sql(biicnn,
                      'update ' + db_if['dt_rql']['full'] +
                      """ set ASSIGNED_BOT = '""" + bot_code + """' 
                     where REQ_ID = """ + str(int(req_info["REQ_ID"])) + """
                     and ASSIGNED_BATCH = """ + str(int(req_batch_id)))


def download_shortage_detail_files(driver, df_req_detail, temp_dl_path, req_id):
    # db_info = o_spt.db_info()
    db_if = o_spt.db_info_new()
    fd_cf = o_spt.folder_config()

    pt = pytz.timezone('US/Pacific')

    shortage_path = fd_cf['shortage_path'] + '\\REQ_' + str(req_id)
    detail_path = fd_cf['detail_path'] + '\\REQ_' + str(req_id)
    pdf_path     = fd_cf['pdf_path']
    spt.generate_folder(shortage_path)
    spt.generate_folder(detail_path)

    # Tải File
    batch_no = 5
    total_requirement = len(df_req_detail)

    status_choose = ['Open', 'Open-recollect', 'Fail']

    total_not_yet = 1
    phase_count = 0
    while phase_count < 2 and total_not_yet > 0:
        df_processing = df_req_detail[((df_req_detail['STATUS_SHORTAGE'].isin(status_choose)) |
                                       (df_req_detail['STATUS_DETAIL'].isin(status_choose)))]
        total_not_yet = len(df_processing)
        print()
        print(
            f'{datetime.datetime.now()} -- Phase {phase_count} : Done {total_requirement - total_not_yet}/{total_requirement} - Remain: {total_not_yet}')
        phase_count = phase_count + 1
        count_full = 0
        count = 0
        invoice_list = ''

        driver.get('https://vendorcentral.amazon.com/hz/vendor/members/coop?ref_=vc_xx_subNav')

        for index, row in df_processing.iterrows():
            invoice_ID = row['INVOICE_NUMBER']
            shortage_status = row['STATUS_SHORTAGE']
            detail_status = row['STATUS_DETAIL']
            # break
            # tạo nhóm Invoice ID cần search
            # Check ID đã download

            if (shortage_status in status_choose) or (detail_status in status_choose):
                count = count + 1
                if count <= batch_no:
                    invoice_list = (invoice_list + invoice_ID + ' , \n')
            count_full = count_full + 1
            # Kiểm tra đủ 1 nhóm yêu cầu thì search
            if count == batch_no or count_full == len(df_processing):
                # break
                # print('Phase: ' + str(abs(count_full/batch_no)) + ' on ' + str(abs(len(df_batch_log)/batch_no)) + ': \n' + invoice_list)
                # search
                driver.implicitly_wait(5)
                search = driver.find_element_by_xpath('//*[@id="search-input"]')
                search.clear()
                search.send_keys(invoice_list)

                driver.implicitly_wait(5)
                check = datetime.datetime.now() + datetime.timedelta(seconds=50)
                while datetime.datetime.now()<= check:
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        sleep(2)
                        driver.find_element_by_xpath(r'//*[@id="search-button-announce"]').click()
                        break
                    except:
                        print(sys.exc_info())
                        pass

                sleep(3)

                # Tải file SHORTAGE theo list seached
                # table = driver.find_elements_by_xpath('//*[@id="cc-invoice-table"]/div[5]/div/table/tbody/tr')# gốc
                #// *[ @ id = "kat-invoice-table"] / table / tbody / tr
                tables = driver.find_elements_by_xpath('//*[@id="cc-invoice-table"]/div[5]/div/table/tbody/tr')
                for table in tables[1:]:
                    # sleep(2)
                    # invoice_num = table[i].find_element_by_id('r' + str(i) +'-INVOICE_NUMBER').text
                    invoice_num = table.find_element_by_xpath('td[2]').text
                    print('\n')
                    print(f'\n a  {invoice_num} \n')
                    print('\n')
                    shortage_status = df_req_detail[df_req_detail['INVOICE_NUMBER'] == invoice_num].iloc[0][
                        'STATUS_SHORTAGE']
                    detail_status = df_req_detail[df_req_detail['INVOICE_NUMBER'] == invoice_num].iloc[0][
                        'STATUS_DETAIL']
                    req_id = df_req_detail[df_req_detail['INVOICE_NUMBER'] == invoice_num].iloc[0]['REQ_ID']
                    # check_status shortage before download
                    try:
                        if shortage_status in status_choose:
                            download_shortage_status = 'Fail'
                            # Tải Shortage
                            try:
                                driver.implicitly_wait(0.5)
                                # xpath="""'r' + str(i) + '-REPORT_DOWNLOADS-input-wrap'"""
                                table.find_element_by_xpath('td[7]').click()
                                # table[i].find_element_by_id('invoiceDownloads-' + invoice_num).click()
                                # driver.find_element_by_id('invoiceDownloads-' + invoice_num).click()
                                # print(download_shortage_status)

                                sleep(0.5)
                                driver.implicitly_wait(1)
                                ele_shortage = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_1')

                                # print(download_shortage_status)
                            except:
                                driver.implicitly_wait(0.5)
                                table.find_elements_by_xpath('/td[7]/div/span').click()
                                # print(download_shortage_status)

                                sleep(0.5)
                                driver.implicitly_wait(1)
                                ele_shortage = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_1')

                                # print(download_shortage_status)

                            if ele_shortage.text == 'Invoice as a spreadsheet':
                                ele_shortage.click()

                                # print(download_shortage_status)

                                # Try đến khi tải đươc file success trong 60s rồi mới đổi file name
                                check_time = datetime.datetime.now() + datetime.timedelta(seconds=60)
                                while datetime.datetime.now() <= check_time:
                                    try:
                                        spt.change_file_name(temp_dl_path, str(invoice_num) + ".csv", shortage_path,
                                                             '.csv')
                                        download_shortage_status = 'Collected'
                                        break
                                    except:
                                        download_shortage_status = 'Fail'
                                        pass

                        else:
                            download_shortage_status = shortage_status
                    except:
                        download_shortage_status = 'Fail'

                    df_req_detail.loc[df_req_detail[df_req_detail[
                                                        'INVOICE_NUMBER'] == invoice_num].index, 'STATUS_SHORTAGE'] = download_shortage_status
                    # Update DB shortage collect status
                    req_dt_info = {}
                    req_dt_info['REQ_ID'] = int(req_id)
                    req_dt_info['STATUS_SHORTAGE'] = download_shortage_status
                    req_dt_info['WITHDRAW_TIME'] = spt.getnow(pt)
                    req_dt_info['INVOICE_NUMBER'] = invoice_num

                    query.json_update_to_db(biicnn, req_dt_info, ['STATUS_SHORTAGE', 'WITHDRAW_TIME'],
                                            ['REQ_ID', 'INVOICE_NUMBER'], db_if['dt_rql']['db'], db_if['dt_rql']['tb'])

                    if detail_status in status_choose:
                        no_report_text = ''
                        try:
                            download_detail_status, df_list_file_name, no_report_text = s_spt.download_detail_files(
                                driver, table,invoice_num, temp_dl_path, detail_path, req_id)
                        except:
                            print(sys.exc_info())
                            download_detail_status = 'Fail'
                        if download_detail_status == 'Collected':
                            # Update DB of list file detail collected
                            # Delete the old with same invoice number & req_ID:
                            query.run_sql(biicnn,
                                          'delete from ' + db_if['dt_fm']['full'] +
                                          ' where REQ_ID = ' + str(req_id)
                                          + " and INVOICE_NUMBER = '" + str(invoice_num) + "'")

                            # Update new file name into db:
                            query.df_to_db(biicnn, df_list_file_name, [], db_if['dt_fm']['db'], db_if['dt_fm']['tb'],
                                           False)

                        if no_report_text == '':
                            no_report_text = None
                        else:
                            no_report_text = "'" + no_report_text + "'"
                        # Update detail collect status
                        req_dt_info = {}
                        req_dt_info['REQ_ID'] = int(req_id)
                        req_dt_info['STATUS_DETAIL'] = download_detail_status
                        req_dt_info['WITHDRAW_TIME'] = spt.getnow(pt)
                        req_dt_info['INVOICE_NUMBER'] = invoice_num
                        req_dt_info['REMARKS'] = no_report_text

                        query.json_update_to_db(biicnn, req_dt_info, ['STATUS_DETAIL', 'WITHDRAW_TIME', 'REMARKS'],
                                                ['REQ_ID', 'INVOICE_NUMBER'], db_if['dt_rql']['db'],
                                                db_if['dt_rql']['tb'])

                        # Update count_no_rec
                        if download_detail_status == 'No_record':
                            # Check if available in no_record table:
                            avai_norec = query.query(biicnn,
                                                     """select sum(count_no_rec) as COUNT_NO_REC 
                                                     from """ + db_if['no_rec']['full'] + """
                                                     where invoice_number = '""" + invoice_num + """'""").iloc[0][
                                'COUNT_NO_REC']
                            new_count_norec = {}
                            new_count_norec['INVOICE_NUMBER'] = invoice_num
                            new_count_norec['LAST_WITHDRAW_TIME'] = spt.getnow(pt)
                            new_count_norec['LAST_REQ_ID'] = int(req_id)
                            if avai_norec == 0 or avai_norec == None:
                                new_count_norec['COUNT_NO_REC'] = 1
                                query.json_to_db(biicnn, new_count_norec, [],
                                                 db_if['no_rec']['db'], db_if['no_rec']['tb'], False)
                            else:
                                new_count_norec['COUNT_NO_REC'] = avai_norec + 1
                                query.json_update_to_db(biicnn, new_count_norec, ['COUNT_NO_REC'], ['INVOICE_NUMBER'],
                                                        db_if['no_rec']['db'], db_if['no_rec']['tb'])

                        df_req_detail.loc[df_req_detail[df_req_detail[
                                                            'INVOICE_NUMBER'] == invoice_num].index, 'STATUS_DETAIL'] = download_detail_status
                    else:
                        download_detail_status = detail_status
                    total_not_yet = len(df_req_detail[((df_req_detail['STATUS_SHORTAGE'].isin(status_choose)) |
                                                       (df_req_detail['STATUS_DETAIL'].isin(status_choose)))])

                    # download pdf
                    sleep(2)
                    invoice_pdf=''
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    try:
                        driver.implicitly_wait(0.5)
                        # xpath="""'r' + str(i) + '-REPORT_DOWNLOADS-input-wrap'"""
                        table.find_element_by_xpath('td[7]').click()
                        # table[i].find_element_by_id('invoiceDownloads-' + invoice_num).click()
                        # driver.find_element_by_id('invoiceDownloads-' + invoice_num).click()
                        # print(download_shortage_status)

                        sleep(0.5)
                        driver.implicitly_wait(1)
                        invoice_pdf = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_0')

                        # print(download_shortage_status)
                    except:
                        pass

                    try:
                        if(invoice_pdf.text == 'Invoice as a PDF'):
                            invoice_pdf.click()
                        sleep(1)
                        check = datetime.datetime.now()+datetime.timedelta(seconds=30)
                        while datetime.datetime.now()<= check:
                            try:
                                spt.change_file_name(temp_dl_path, str(invoice_num) + ".pdf", pdf_path,
                                             '.pdf')
                                break
                            except:
                                pass
                    except:
                        pass
                    print(
                        f'\r-- {datetime.datetime.now()} - Done {total_requirement - total_not_yet}/{total_requirement} - last {invoice_num}: ST-{download_shortage_status} : DT-{download_detail_status} -',
                        end='\r')

                count = 0
                invoice_list = ''


def download_detail_files(driver, table, invoice_num, temp_dl_path, detail_path, req_id):
    df_list_file_name = pd.DataFrame([], columns=['REQ_ID', 'INVOICE_NUMBER', 'FILE_NAME', 'WITHDRAW_TIME'])
    download_status = 'Fail'

    import shutil
    # sleep_time  = 3
    pt = pytz.timezone('US/Pacific')

    temp_dl_path_2 = temp_dl_path + '_2'
    try:
        os.mkdir(temp_dl_path_2)
    except:
        pass

    try:
        driver.implicitly_wait(1)
        # table_i.find_element_by_id('invoiceDownloads-'+ invoice_num + '-announce').click()
        table.find_element_by_xpath('td[7]').click()

        sleep(0.5)
        driver.implicitly_wait(1)
        ele_shortage = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_2')
    except:
        driver.implicitly_wait(1)
        # table_i.find_element_by_id('invoiceDownloads-'+ invoice_num + '-announce').click()
        table.find_element_by_xpath('td[7]').click()
        sleep(0.5)
        driver.implicitly_wait(1)
        ele_shortage = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_2')

    if ele_shortage.text == 'Backup report':
        ele_shortage.click()
    sleep(5)

    no_report_text = ''
    # Click Download and close table download

    # Check no report available
    try:
        driver.implicitly_wait(2)
        no_report_text = driver.find_element_by_xpath(
            '//*[@id="backup-report-not-found"]/div[@class="a-box-inner a-alert-container"]/div[@class="a-alert-content"]').text
    except:
        try:
            driver.implicitly_wait(2)
            no_report_text = driver.find_element_by_xpath('//*[@id="backup-report-not-supported"]').text
        except:
            try:
                driver.implicitly_wait(1)
                no_report_text = driver.find_element_by_xpath('//*[@id="backup-report-no-records"]').text
            except:
                no_report_text = ''

    print(f'no report text:  {no_report_text}')
    if no_report_text != '':
        download_status = 'No_record'

    else:
        try:
            try_get_tb = 0
            number_file = 1
            while number_file == 1 and try_get_tb < 5:
                table_detail = driver.find_elements_by_xpath('//*[@id="backup-report-table"]/tbody/tr')
                number_file = len(table_detail)
                sleep(5)

            count_downloaded = 0
            spt.clean_folder(temp_dl_path)
            spt.clean_folder(temp_dl_path_2)

            for j in range(1, len(table_detail)):
                driver.implicitly_wait(5)
                table_detail[j].find_element_by_tag_name('a').click()
                check_time = datetime.datetime.now() + datetime.timedelta(seconds=60)
                file_info = {}
                file_name = None
                while datetime.datetime.now() <= check_time:
                    try:
                        if len(table_detail) > 2:
                            file_name = str(invoice_num) + '_' + str(j) + '_' + str(len(table_detail) - 1) + '.xls'
                            spt.change_file_name(temp_dl_path, file_name, temp_dl_path_2, '.xls')
                        else:
                            file_name = str(invoice_num) + ".xls"
                            spt.change_file_name(temp_dl_path, file_name, temp_dl_path_2, '.xls')
                        count_downloaded = count_downloaded + 1
                        file_info['FILE_NAME'] = file_name
                        file_info['WITHDRAW_TIME'] = datetime.datetime.now(tz=pt).replace(tzinfo=None)
                        df_list_file_name = df_list_file_name.append(file_info, ignore_index=True)
                        break
                    except:
                        # download_status = "Fail"
                        pass
        except:
            pass

        # Thoát màn  hình download, ghi nhận status & next
        if count_downloaded == (len(table_detail) - 1) and count_downloaded > 0:
            df_list_file_name['INVOICE_NUMBER'] = invoice_num
            df_list_file_name['REQ_ID'] = req_id
            for detail_file_name in df_list_file_name['FILE_NAME']:
                shutil.move(os.path.join(temp_dl_path_2, detail_file_name), os.path.join(detail_path, detail_file_name))
            download_status = 'Collected'
        else:
            for detail_file_name in df_list_file_name['FILE_NAME']:
                try:
                    os.remove(os.path.join(temp_dl_path_2, detail_file_name))
                except:
                    pass
            download_status = 'Fail'
            df_list_file_name = pd.DataFrame([], columns=['REQ_ID', 'INVOICE_NUMBER', 'FILE_NAME', 'WITHDRAW_TIME'])

    driver.implicitly_wait(5)
    driver.find_element_by_xpath('//*[@id="return-to-invoice-overview"]/span').click()
    return download_status, df_list_file_name, no_report_text


def open_excel_sheet(excel_file, sheet_name, ignore_cols):
    # Read excel: default UPC, EAN, Category, subCategory as str type
    df_input = pd.read_excel(excel_file, sheet_name,
                             converters={'UPC': str, 'EAN': str,
                                         'Category': str, 'Subcategory': str})
    # number_of_cols = len(df_input.columns)

    # Update cols name
    ori_cols = df_input.columns
    new_ori_cols = []
    new_cols = []
    for col in ori_cols:
        new_col = col.upper()
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('-', '_')
        new_col = new_col.replace('#', 'NUMBER')
        if len(new_col) > 30:
            new_col = new_col[0:30]
        if new_col not in ignore_cols:
            # print(new_col)
            new_cols = new_cols + [new_col]
            new_ori_cols = new_ori_cols + [col]

    df_input = df_input[new_ori_cols]
    df_input.columns = new_cols
    return df_input


def process_detail_file(req_id, file_name, invoice_number, withdraw_time):
    not_def_check = 0
    remark_text = ''
    type_match_text = ''

    type_match_list = []
    remark_list = []

    man_cols_list = ['ID', 'REQ_ID', 'WITHDRAW_TIME', 'INVOICE_NUMBER']
    # Get type code info
    # Trả về list danh sách trong table E01_table_trans
    type_info = s_spt.get_list_ori_cols(man_cols_list)
    """typ"""

    # {'CLF': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'PROMOTION_ID', 'TRANS_TABLE': 'E01_ORI_CLIP_FULLOG',
    #          'TRANS_DB': 'SYSTEM', 'DEST_TABLE': 'E01_COIN_CLIP_FULLOG', 'DEST_DB': 'SYSTEM',
    #          'LIST_COLS': ['PROMOTION_ID', 'PROMOTION_DESCRIPTION', 'CLIP_DAY', 'CLIPPED_COUPONS', 'CLIP_SOURCE',
    #                        'CLIP_FEE', 'TOTAL_CLIP_FEE']},
    #  'PMC_1': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'REBATE', 'TRANS_TABLE': 'E01_ORI_PROMOTION_T1_FULLOG',
    #            'TRANS_DB': 'SYSTEM', 'DEST_TABLE': 'E01_COIN_PROMOTION_FULLOG', 'DEST_DB': 'SYSTEM',
    #            'LIST_COLS': ['ORDER_DATE', 'SHIP_DATE', 'RETURN_DATE', 'COST_DATE', 'TRANSACTION_TYPE', 'QUANTITY',
    #                          'NET_SALES', 'NET_SALES_CURRENCY', 'LIST_PRICE', 'LIST_PRICE_CURRENCY', 'REBATE',
    #                          'REBATE_CURRENCY', 'PURCHASE_ORDER', 'ASIN', 'UPC', 'EAN', 'MANUFACTURER', 'DISTRIBUTOR',
    #                          'PRODUCT_GROUP', 'CATEGORY', 'SUBCATEGORY', 'TITLE', 'BINDING', 'PROMOTION_ID',
    #                          'COST_TYPE', 'ORDER_COUNTRY']},
    #  'PMC_2': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'REBATE_IN_AGREEMENT_CURRENCY',
    #            'TRANS_TABLE': 'E01_ORI_PROMOTION_T2_FULLOG', 'TRANS_DB': 'SYSTEM',
    #            'DEST_TABLE': 'E01_COIN_PROMOTION_FULLOG', 'DEST_DB': 'SYSTEM',
    #            'LIST_COLS': ['ORDER_DATE', 'SHIP_DATE', 'RETURN_DATE', 'COST_DATE', 'TRANSACTION_TYPE', 'QUANTITY',
    #                          'NET_SALES', 'NET_SALES_CURRENCY', 'LIST_PRICE', 'LIST_PRICE_CURRENCY',
    #                          'REBATE_IN_AGREEMENT_CURRENCY', 'AGREEMENT_CURRENCY', 'REBATE_IN_PURCHASE_ORDER_CURRE',
    #                          'PURCHASE_ORDER_CURRENCY', 'PURCHASE_ORDER', 'ASIN', 'UPC', 'EAN', 'MANUFACTURER',
    #                          'DISTRIBUTOR', 'PRODUCT_GROUP', 'CATEGORY', 'SUBCATEGORY', 'TITLE', 'BINDING',
    #                          'PROMOTION_ID', 'COST_TYPE', 'ORDER_COUNTRY']},
    #  'CSF_1': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'REBATE', 'TRANS_TABLE': 'E01_ORI_CSF_T1_FULLOG',
    #            'TRANS_DB': 'SYSTEM', 'DEST_TABLE': 'E01_COIN_CSF_FULLOG', 'DEST_DB': 'SYSTEM',
    #            'LIST_COLS': ['RECEIVE_DATE', 'RETURN_DATE', 'INVOICE_DAY', 'TRANSACTION_TYPE', 'QUANTITY',
    #                          'NET_RECEIPTS', 'NET_RECEIPTS_CURRENCY', 'LIST_PRICE', 'LIST_PRICE_CURRENCY', 'REBATE',
    #                          'REBATE_CURRENCY', 'PURCHASE_ORDER', 'ASIN', 'UPC', 'EAN', 'MANUFACTURER', 'DISTRIBUTOR',
    #                          'PRODUCT_GROUP', 'CATEGORY', 'SUBCATEGORY', 'TITLE', 'BINDING', 'COST_CURRENCY',
    #                          'OLD_COST', 'NEW_COST', 'PRICE_PROTECTION_AGREEMENT', 'PRICE_PROTECTION_DAY',
    #                          'COST_VARIANCE', 'INVOICE']},
    #  'CSF_2': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'REBATE_IN_AGREEMENT_CURRENCY',
    #            'TRANS_TABLE': 'E01_ORI_CSF_T2_FULLOG', 'TRANS_DB': 'SYSTEM', 'DEST_TABLE': 'E01_COIN_CSF_FULLOG',
    #            'DEST_DB': 'SYSTEM',
    #            'LIST_COLS': ['PURCHASE_ORDER', 'ASIN', 'UPC', 'EAN', 'MANUFACTURER', 'DISTRIBUTOR', 'PRODUCT_GROUP',
    #                          'CATEGORY', 'SUBCATEGORY', 'TITLE', 'BINDING', 'COST_CURRENCY', 'OLD_COST', 'NEW_COST',
    #                          'PRICE_PROTECTION_AGREEMENT', 'PRICE_PROTECTION_DAY', 'COST_VARIANCE', 'INVOICE',
    #                          'RECEIVE_DATE', 'RETURN_DATE', 'INVOICE_DAY', 'TRANSACTION_TYPE', 'QUANTITY',
    #                          'NET_RECEIPTS', 'NET_RECEIPTS_CURRENCY', 'LIST_PRICE', 'LIST_PRICE_CURRENCY',
    #                          'REBATE_IN_AGREEMENT_CURRENCY', 'AGREEMENT_CURRENCY', 'REBATE_IN_PURCHASE_ORDER_CURRE',
    #                          'PURCHASE_ORDER_CURRENCY']},
    #  'CSF_3': {'TRANSIT_REQUIREMENT': 1, 'NOT_NULL_COL': 'REVISED_INVOICE_REBATE',
    #            'TRANS_TABLE': 'E01_ORI_CSF_T3_FULLOG', 'TRANS_DB': 'SYSTEM', 'DEST_TABLE': 'E01_COIN_CSF_FULLOG',
    #            'DEST_DB': 'SYSTEM',
    #            'LIST_COLS': ['RECEIVE_DATE', 'RETURN_DATE', 'INVOICE_DAY', 'TRANSACTION_TYPE', 'NET_RECEIPTS',
    #                          'NET_RECEIPTS_CURRENCY', 'LIST_PRICE', 'LIST_PRICE_CURRENCY', 'REVISED_INVOICE_QUANTITY',
    #                          'REVISED_INVOICE_REBATE', 'REBATE_CURRENCY', 'NEW_QUANTITY', 'NEW_REBATE',
    #                          'OLD_INVOICE_QUANTITY', 'OLD_INVOICE_REBATE', 'PURCHASE_ORDER', 'ASIN', 'UPC', 'EAN',
    #                          'MANUFACTURER', 'DISTRIBUTOR', 'PRODUCT_GROUP', 'CATEGORY', 'SUBCATEGORY', 'TITLE',
    #                          'BINDING', 'COST_CURRENCY', 'OLD_COST', 'NEW_COST', 'PRICE_PROTECTION_AGREEMENT',
    #                          'PRICE_PROTECTION_DAY', 'COST_VARIANCE', 'INVOICE']},
    #  'RDF': {'TRANSIT_REQUIREMENT': 0, 'NOT_NULL_COL': 'PROMOTION_ID', 'TRANS_TABLE': 'E01_ORI_REDEMTION_FULLOG',
    #          'TRANS_DB': 'SYSTEM', 'DEST_TABLE': None, 'DEST_DB': None,
    #          'LIST_COLS': ['PROMOTION_ID', 'PROMOTION_DESCRIPTION', 'COUPON_CODE', 'ORDER_DAY', 'TRANSACTION_ID',
    #                        'CUSTOMER_ID', 'ASIN', 'UPC', 'QUANTITY', 'COUPON_FACE_VALUE', 'REDEMPTION_FEE']}}

    list_data, check_collect = s_spt.open_excel_file(req_id, invoice_number, file_name, withdraw_time)

    if check_collect == True:
        for data_info in list_data:
            type_match = s_spt.check_type_detailsheet(data_info, type_info)
            # print(f'{file_name} - {type_match} -')
            if type_match != '_Null_':
                # Add man cols to df:
                df_data = data_info['df']
                df_data['REQ_ID'] = req_id
                df_data['WITHDRAW_TIME'] = withdraw_time
                df_data['INVOICE_NUMBER'] = invoice_number
                not_null_col = type_info[type_match]['NOT_NULL_COL']
                index_null = df_data[df_data[not_null_col].isnull()].index.min()
                df_data = df_data.head(index_null)

                s_spt.update_transittb_df_to_fullog(req_id, df_data, invoice_number,
                                                    type_info[type_match]['TRANS_TABLE'])
                s_spt.update_transittb_fullog_to_master(req_id, invoice_number, type_info[type_match]['TRANS_TABLE'])
            else:
                not_def_check = 1
            type_match_list = type_match_list + ["['" + type_match + "']"]
            remark = "{'Sheet_name': '" + data_info['sheet_name'] + "' , 'Type_code': '" + type_match + "'}"
            remark_list = remark_list + [remark]
    else:
        type_match_list = type_match_list + ["['NC']"]

    type_match_text = ','.join(type_match_list)
    remark_text = ','.join(remark_list)

    return not_def_check, remark_text, type_match_text


def open_excel_file(req_id, invoice_number, file_name, withdraw_time):
    fd_cf = o_spt.folder_config()
    ignore_cols = ['PRODUCT_DESCRIPTION','MULTI_COUNTRY_PARENT_AGREEMENT']
    detail_path = fd_cf['detail_path'] + '\\' + 'REQ_' + str(req_id)
    import os
    os.chdir(detail_path)

    try:
        excel_file = pd.ExcelFile(file_name)
        check_collect = True
    except:
        check_collect = False

    list_data = []
    if check_collect == True:
        for sheet_name in excel_file.sheet_names:
            df_input = s_spt.open_excel_sheet(excel_file, sheet_name, ignore_cols)
            new_data_cont = {'file_name': file_name,
                             'sheet_name': sheet_name,
                             'withdraw_time': withdraw_time,
                             'invocie_number': invoice_number,
                             'req_id': req_id,
                             'df': df_input}
            list_data = list_data + [new_data_cont]

    return list_data, check_collect


def get_list_ori_cols(man_cols_list):
    db_if = o_spt.db_info_new()
    # fd_cf = o_spt.folder_config()

    # Select tatble
    # table_name = db_if['dt_tb']['full']
    # select table
    df = query.query(biicnn, 'select * from ' + db_if['dt_tb']['full'] + " where STORAGE_REQUIREMENT = 1")

    type_info = {}
    for index, row in df.iterrows():
        type_code = row['TYPE_CODE']
        type_info[type_code] = {}
        type_info[type_code]['TRANSIT_REQUIREMENT'] = row['TRANSIT_REQUIREMENT']
        type_info[type_code]['NOT_NULL_COL'] = row['NOT_NULL_COL']
        type_info[type_code]['TRANS_TABLE'] = row['TRANS_TABLE']
        type_info[type_code]['TRANS_DB'] = s_spt.get_db_name(row['TRANS_TABLE'])
        type_info[type_code]['DEST_TABLE'] = row['DEST_TABLE']
        if type_info[type_code]['DEST_TABLE'] == None:
            type_info[type_code]['DEST_DB'] = None
        else:
            type_info[type_code]['DEST_DB'] = s_spt.get_db_name(row['DEST_TABLE'])

        # get list cols of table:
        df_cols = query.query(biicnn,
                              """SELECT  table_name, column_name, data_type 
                              FROM all_tab_columns 
                              where table_name = '""" + type_info[type_code]['TRANS_TABLE'] + "' and owner = '" +
                              type_info[type_code]['TRANS_DB'] + "'")

        list_cols = []
        for col in df_cols['COLUMN_NAME']:
            if col not in man_cols_list:
                list_cols = list_cols + [col]

        type_info[type_code]['LIST_COLS'] = list_cols

    return type_info


def get_db_name(tb_name):
    db_if = o_spt.db_info_new()
    for key, value in db_if.items():
        if db_if[key]['tb'] == tb_name:
            db_name = db_if[key]['db']

    return db_name


def check_type_detailsheet(data_info, type_info):
    type_match = '_Null_'
    df_input = data_info['df']
    num_cols_input = len(df_input.columns)
    count_match = 0
    for key, value in type_info.items():
        # print(key)
        # print(len(type_info[key]['LIST_COLS']))
        if len(type_info[key]['LIST_COLS']) == num_cols_input:
            # print('match number of cols: ' + str(key))
            for in_col in df_input.columns:
                # print('\t ' + in_col)
                for out_col in type_info[key]['LIST_COLS']:
                    # print('\t ' + out_col)
                    if in_col == out_col:
                        count_match = count_match + 1
                        # print('\t matched')
                        break

            if count_match == num_cols_input:
                type_match = key
                break
    # print('match ' + str(count_match) + ' / ' + str(num_cols_input))
    return type_match


def update_transittb_df_to_fullog(req_id, df_data, invoice_number, ora_fullog_table):
    db_name = s_spt.get_db_name(ora_fullog_table)
    # df =query.query(biicnn,f"""select INVOICE_NUMBER ,COUNT(distinct("TYPE")) as COUNT_TYPE , COUNT(0) as COUNT_NO
    #     from system.E01_COIN_DETAIL_FILEMAN ecdf    where INVOICE_NUMBER ='{invoice_number}' group by INVOICE_NUMBER """)
    # count_type =0
    # count_no =0
    # if(len(df)>0):
    #     count_type = df.iloc[0]['COUNT_TYPE']
    #     count_no = df.iloc[0]['COUNT_NO']
    #
    # if count_type == count_no:
    query.run_sql(biicnn, """DELETE FROM  """ + db_name + '.' + ora_fullog_table +
              """ WHERE REQ_ID = """ + str(req_id) +
              """ and INVOICE_NUMBER = '""" + str(invoice_number) + "'")

    query.df_to_db(biicnn, df_data, ['ID'], db_name, ora_fullog_table, False)


def update_transittb_fullog_to_master(req_id, invoice_number, fl_tb):
    mt_tb = fl_tb.replace('_FULLOG', '_MASTER')

    mt_db = s_spt.get_db_name(mt_tb)
    fl_db = s_spt.get_db_name(fl_tb)

    # select data type from ora table
    df_cols = query.query(biicnn,
                          """SELECT  table_name, column_name, data_type 
                             FROM all_tab_columns 
                             where table_name = '""" + fl_tb + "' and owner = '" + fl_db + "'")

    transf_cols = []
    for col in df_cols['COLUMN_NAME']:
        transf_cols = transf_cols + [col]

    transf_cols_text = ','.join(transf_cols)

    # print([mt_db,mt_tb,transf_cols_text,fl_db,fl_tb,req_id,invoice_number])

    sql_delete = (
                """DELETE FROM  """ + mt_db + '.' + mt_tb + """ WHERE INVOICE_NUMBER = '""" + str(invoice_number) + "'")
    sql_insert = ("""INSERT INTO """ + mt_db + '.' + mt_tb + "(""" + transf_cols_text + """) 
                  SELECT """ + transf_cols_text + """ FROM """ + fl_db + '.' + fl_tb + """ WHERE REQ_ID = """ + str(
        req_id) + """ and INVOICE_NUMBER = '""" + invoice_number + "'")

    cnn, cursor = biicnn.connection()
    cursor.execute(sql_delete)
    cursor.execute(sql_insert)
    cnn.commit()
    cnn = None
    sql_insert = None


def update_file_type_info_to_db(req_id, invoice_number, file_name, not_def_check, remark_text, type_match_text):
    db_if = o_spt.db_info_new()

    if remark_text == '':
        remark_text = 'Null'
    else:
        remark_text = remark_text.replace("'", "''")
        remark_text = "'" + remark_text + "'"
    if type_match_text == '':
        type_match_text = 'Null'
    else:
        type_match_text = type_match_text.replace("'", "''")
        type_match_text = "'" + type_match_text + "'"

    query.run_sql(biicnn,
                  'update ' + db_if['dt_fm']['full'] +
                  """ set TYPE = """ + type_match_text + """, 
                    NOT_DEF_CHECK = """ + str(int(not_def_check)) + """, 
                    REMARKS= """ + remark_text + """ 
                    where REQ_ID = """ + str(int(req_id)) + """ 
                    and INVOICE_NUMBER = '""" + invoice_number + """' 
                    and FILE_NAME = '""" + file_name + """'""")


def update_req_detail_list_status(req_id, invoice_number, status_type):
    db_if = o_spt.db_info_new()
    query.run_sql(biicnn,
                  'update ' + db_if['dt_rql']['full'] +
                  """ set """ + status_type + """ = 'Complete' 
                                    where REQ_ID = """ + str(int(req_id)) + """ 
                                    and INVOICE_NUMBER = '""" + invoice_number + """'""")


def get_list_file_by_invoicenumber(req_id, invoice_number):
    # Select tatble
    db_if = o_spt.db_info_new()
    # select table
    df = query.query(biicnn,
                     'select * from ' + db_if['dt_fm']['full'] +
                     " where INVOICE_NUMBER = '" + invoice_number +
                     "' and REQ_ID = " + str(req_id))

    return df


def get_req_detail_list(req_id, spec_assigned_bot):
    db_if = o_spt.db_info_new()
    # select table
    if spec_assigned_bot == '':
        sql_select = 'select * from ' + db_if['dt_rql']['full'] + " where REQ_ID = " + str(req_id)
    else:
        sql_select = 'select * from ' + db_if['dt_rql']['full'] + " where REQ_ID = " + str(
            req_id) + " and ASSIGNED_BOT = '" + str(spec_assigned_bot) + "'"

    df_req_detail = query.query(biicnn, sql_select)

    return df_req_detail


def update_dest_fullog(req_id, announce):
    db_if = o_spt.db_info_new()

    done_list = []
    tranman_table = query.query(biicnn, 'select * from ' + db_if['dt_tb']['full'])
    tranman_cols = query.query(biicnn, 'select * from ' + db_if['dt_cl']['full'])

    if announce == True:
        print('--------- UPDATE COST TABLE ---------')
    for index, row in tranman_table.iterrows():

        transit_requirement = row['TRANSIT_REQUIREMENT']
        type_code = row['TYPE_CODE']

        if transit_requirement == 1:
            ori_table = row['TRANS_TABLE']
            ori_db = s_spt.get_db_name(ori_table)
            dest_table = row['DEST_TABLE']
            if dest_table == None:
                dest_db = None
            else:
                dest_db = s_spt.get_db_name(dest_table)
            # print(f'Type [{type_code}] : from table [{ori_table}] to dest table [{dest_table}]' )
            tranman_cols_spec = tranman_cols[tranman_cols['TYPE_CODE'] == type_code]

            ori_cols = ','.join(tranman_cols_spec['TRANS_COL'])
            dest_cols = ','.join(tranman_cols_spec['DEST_COL'])

            # Delete the same req info in dest table:
            sql_delete = ("""DELETE FROM  """ + dest_db + '.' + dest_table +
                          """ WHERE REQ_ID = """ + str(req_id) + """ and REF_TYPE = '""" + type_code + "'")

            # import new req info into dest table
            # if announce == True:
            # print(f'{datetime.datetime.now()} - Import table {dest_table}')"""
            sql_insert = ('insert into ' + dest_db + '.' + dest_table +
                          ' (REF_TYPE, ' + dest_cols + """) 
                            select """ + "'" + type_code + "', " + ori_cols +
                          ' from ' + ori_db + '.' + ori_table + ' where REQ_ID = ' + str(req_id))
            # print(sql_delete + ';')
            # print(sql_insert + ';')

            cnn, cursor = biicnn.connection()
            cursor.execute(sql_delete)
            cursor.execute(sql_insert)
            cnn.commit()

            cnn = None
            sql_delete = None
            sql_insert = None

            if announce == True:
                print(f'{datetime.datetime.now()} - Done Import table {dest_table}')

            done_list = done_list + [[type_code, ori_table, dest_table]]
    return done_list


def update_dest_master(req_id):
    db_if = o_spt.db_info_new()
    announce = True
    done_list = []

    dest_table_list = query.query(biicnn,
                                  'select distinct(DEST_TABLE) from ' + db_if['dt_tb']['full'] +
                                  ' WHERE TRANSIT_REQUIREMENT = 1')

    for table in dest_table_list['DEST_TABLE']:
        fullog_table = table
        fl_db = s_spt.get_db_name(fullog_table)
        master_table = table.replace('_FULLOG', '_MASTER')
        mt_db = s_spt.get_db_name(master_table)
        # print( fullog_table + ' ' + master_table)
        # select data type from ora table
        df_cols = query.query(biicnn,
                              """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                              where table_name = '""" + fullog_table + "' and owner = '" + fl_db + "'")

        transf_cols = []
        for col in df_cols['COLUMN_NAME']:
            transf_cols = transf_cols + [col]

        transf_cols_text = ','.join(transf_cols)

        sql_delete = ("""DELETE FROM  """ + mt_db + '.' + master_table +
                      """ WHERE INVOICE_NUMBER IN (SELECT DISTINCT(INVOICE_NUMBER) 
                                                  FROM """ + fl_db + '.' + fullog_table +
                      " WHERE REQ_ID = " + str(req_id) + ")")
        sql_insert = ('insert into ' + mt_db + '.' + master_table + ' (' + transf_cols_text +
                      ') select ' + transf_cols_text + ' from ' + fl_db + '.' + fullog_table + " WHERE REQ_ID = " + str(
                    req_id))

        # RUN SQL INPORT
        if announce == True:
            print(f'\t ---- {datetime.datetime.now()} - Import table {master_table}')
        print(sql_delete)
        print(sql_insert)
        cnn, cursor = biicnn.connection()
        cursor.execute(sql_delete)
        cursor.execute(sql_insert)
        cnn.commit()
        cnn = None
        sql_insert = None
        if announce == True:
            print(f'\t ---- {datetime.datetime.now()} - Done')

        done_list = done_list + [[fullog_table, master_table]]
    return done_list


def open_shortage_file(req_id, invoice_number):
    # import time
    fd_cf = o_spt.folder_config()
    ignore_cols = []
    os.chdir(fd_cf['shortage_path'] + '\\' + 'REQ_' + str(req_id))

    dfcolumns = pd.read_csv(invoice_number + '.csv', nrows=1)
    df_input = pd.read_csv(invoice_number + '.csv',
                           header=None,
                           skiprows=1,
                           usecols=list(range(len(dfcolumns.columns))),
                           names=dfcolumns.columns)

    ori_cols = df_input.columns
    new_ori_cols = []
    new_cols = []
    for col in ori_cols:
        new_col = col.upper()
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('#', 'NUMBER')
        if len(new_col) > 30:
            new_col = new_col[0:30]
        if new_col not in ignore_cols:
            # print(new_col)
            new_cols = new_cols + [new_col]
            new_ori_cols = new_ori_cols + [col]

    df_input = df_input[new_ori_cols]
    df_input.columns = new_cols

    stat = os.stat(invoice_number + '.csv')
    withdraw_time = datetime.datetime.fromtimestamp(stat.st_ctime)

    return df_input, withdraw_time


def run_table_validation():
    db_if = o_spt.db_info_new()
    sqls_type = [
        """merge into """ + db_if['ovr_mt']['full'] + """ mt
                    using (select funding_agreement as AGREEMENT_ID, PROMOTION_ID 
                            from """ + db_if['np']['full'] + """) p
                    on (mt.AGREEMENT_ID = p.AGREEMENT_ID)
                    WHEN MATCHED THEN
                          UPDATE SET mt.TYPE_INFO = 'NORMAL_PROMOTION_FUNDING_AGREEMENT',
                                      mt.FEE_ID = p.PROMOTION_ID  
                          WHERE mt.TYPE_INFO  IS NULL OR mt.TYPE_INFO IN ('PROMOTION', 'COUPON_PROMOTION')"""
        ,
        """merge into """ + db_if['ovr_mt']['full'] + """ mt
                using (select FEE_AGREEMENT as AGREEMENT_ID, PROMOTION_ID 
                        from """ + db_if['np']['full'] + """ WHERE FEE_AGREEMENT IS NOT NULL) p
                on (mt.AGREEMENT_ID = p.AGREEMENT_ID)
                WHEN MATCHED THEN
                      UPDATE SET mt.TYPE_INFO = 'NORMAL_PROMOTION_FEE_AGREEMENT',
                                  mt.FEE_ID = p.PROMOTION_ID  
                      WHERE mt.TYPE_INFO  IS NULL OR mt.TYPE_INFO IN ('PROMOTION', 'COUPON_PROMOTION')"""
        ,
        """UPDATE """ + db_if['ovr_mt']['full'] + """ SET TYPE_INFO = 'COUPON_PROMOTION'
                WHERE   
                    INVOICE_ID IN (SELECT DISTINCT(INVOICE_NUMBER) FROM """ + db_if['clip_mt']['full'] + """)
                    AND (TYPE_INFO IS NULL or TYPE_INFO = 'PROMOTION')"""
        ,
        """UPDATE """ + db_if['ovr_mt']['full'] + """ SET TYPE_INFO = 'COUPON_PROMOTION'
                WHERE   
                    INVOICE_ID IN (SELECT DISTINCT(INVOICE_NUMBER) FROM """ + db_if['rdt_o_mt']['full'] + """)
                    AND (TYPE_INFO IS NULL or TYPE_INFO = 'PROMOTION')"""
        ,
        """UPDATE """ + db_if['ovr_mt']['full'] + """ SET TYPE_INFO = 'PROMOTION'
                WHERE   
                    INVOICE_ID IN (SELECT DISTINCT(INVOICE_NUMBER) FROM """ + db_if['pr_mt']['full'] + """)
                    AND TYPE_INFO IS NULL"""
        ,
        """UPDATE """ + db_if['ovr_mt']['full'] + """ SET TYPE_INFO = 'CHANNEL_SELLING_FEE'
                WHERE   
                    INVOICE_ID IN (SELECT DISTINCT(INVOICE_NUMBER) FROM """ + db_if['csf_mt']['full'] + """)
                    AND TYPE_INFO IS NULL"""
        ,
        """
        merge into """ + db_if['ovr_mt']['full'] + """ des
                using (
                        SELECT invoice_id, CAMPAIGN_ID FROM
                            (select ovr.invoice_id, cp.CAMPAIGN_ID  
                            from """ + db_if['ovr_mt']['full'] + """ ovr
                            left join """ + db_if['clip_mt']['full'] + """ clip on ovr.INVOICE_ID = clip.INVOICE_NUMBER
                            left join """ + db_if['cp_adi_mt']['full'] + """ cp on cp.COUPON_ID = clip.PROMOTION_ID
                            where ovr.type_info = 'COUPON_PROMOTION'
                              and clip.INVOICE_NUMBER is not null
                              and cp.COUPON_ID is not null) TB1
                          GROUP BY invoice_id, CAMPAIGN_ID) cp_info
                ON (des.invoice_id = cp_info.invoice_id)
                when matched then
                update set des.FEE_ID = cp_info.CAMPAIGN_ID,des.TYPE_INFO = 'COUPON_PROMOTION'
                where des.FEE_ID IS NULL OR des.TYPE_INFO IS NULL OR des.TYPE_INFO = 'PROMOTION'"""
        ,
        """
        merge into """ + db_if['ovr_mt']['full'] + """ des
                using (
                        SELECT invoice_id, CAMPAIGN_ID FROM
                            (select ovr.invoice_id, cp.CAMPAIGN_ID  
                            from """ + db_if['ovr_mt']['full'] + """ ovr
                            left join """ + db_if['rdt_o_mt']['full'] + """ red on ovr.INVOICE_ID = red.INVOICE_NUMBER
                            left join """ + db_if['cp_adi_mt']['full'] + """ cp on cp.COUPON_ID = red.PROMOTION_ID
                            where ovr.type_info = 'COUPON_PROMOTION'
                              and red.INVOICE_NUMBER is not null
                              and cp.COUPON_ID is not null) TB1
                          GROUP BY invoice_id, CAMPAIGN_ID) cp_info
                ON (des.invoice_id = cp_info.invoice_id)
                when matched then
                update set des.FEE_ID = cp_info.CAMPAIGN_ID,des.TYPE_INFO = 'COUPON_PROMOTION'
                where des.FEE_ID IS NULL OR des.TYPE_INFO IS NULL OR des.TYPE_INFO = 'PROMOTION'"""

    ]

    sqls = [
        """DELETE FROM """ + db_if['coop_vali']['full'] + """ 
            WHERE DIFF_OVR_DET IS NULL OR DIFF_OVR_DET <> 0
            """  # Upddate new on 9/30/2021
        ,
        """INSERT INTO """ + db_if['coop_vali']['full'] + """ 
                  (INVOICE_NUMBER, INVOICE_DATE, AGREEMENT_ID, AGREEMENT_TITLE, 
                  TYPE_INFO,FEE_ID,ORIGINAL_BALANCE,
                  REDEMPTION_FEE,CLIP_FEE,PROMOTION_COST,CHANNEL_SELLING_FEE,MIN_COST_DATE,MAX_COST_DATE)
                select ovr.INVOICE_ID, ovr.INVOICE_DATE, ovr.AGREEMENT_ID, ovr.AGREEMENT_TITLE, 
                        ovr.TYPE_INFO, ovr.FEE_ID, ovr.ORIGINAL_BALANCE,
                        red.REDEMPTION_FEE, clip.CLIP_FEE, prm.PROMOTION_COST,  csf.CHANNEL_SELLING_FEE,

                least(    COALESCE(clip.MIN_COST_DATE,prm.MIN_COST_DATE, red.MIN_COST_DATE,csf.MIN_COST_DATE),
                          COALESCE(prm.MIN_COST_DATE, red.MIN_COST_DATE,csf.MIN_COST_DATE,clip.MIN_COST_DATE),
                          COALESCE(red.MIN_COST_DATE,csf.MIN_COST_DATE,clip.MIN_COST_DATE,prm.MIN_COST_DATE),
                          COALESCE(csf.MIN_COST_DATE,clip.MIN_COST_DATE,prm.MIN_COST_DATE,red.MIN_COST_DATE)) as MIN_COST_DATE,

                greatest( COALESCE(clip.MAX_COST_DATE,prm.MAX_COST_DATE, red.MAX_COST_DATE,csf.MAX_COST_DATE),
                          COALESCE(prm.MAX_COST_DATE, red.MAX_COST_DATE,csf.MAX_COST_DATE,clip.MAX_COST_DATE),
                          COALESCE(red.MAX_COST_DATE,csf.MAX_COST_DATE,clip.MAX_COST_DATE,prm.MAX_COST_DATE),
                          COALESCE(csf.MAX_COST_DATE,clip.MAX_COST_DATE,prm.MAX_COST_DATE,red.MAX_COST_DATE)) as MAX_COST_DATE
                from """ + db_if['ovr_mt']['full'] + """ ovr
                left join (select INVOICE_NUMBER,  SUM(TOTAL_CLIP_FEE)AS CLIP_FEE,
                            MIN(CLIP_DAY) AS MIN_COST_DATE ,MAX(CLIP_DAY) AS MAX_COST_DATE
                            from  """ + db_if['clip_mt']['full'] + """
                            GROUP BY INVOICE_NUMBER) clip 
                            on clip.INVOICE_NUMBER =  ovr.INVOICE_ID
                left join (select INVOICE_NUMBER, SUM(PROMOTION_COST) AS PROMOTION_COST,
                            MIN(ORDER_DATE) AS MIN_COST_DATE ,MAX(ORDER_DATE) AS MAX_COST_DATE
                            from  """ + db_if['pr_mt']['full'] + """
                            GROUP BY INVOICE_NUMBER) prm
                            on prm.INVOICE_NUMBER =  ovr.INVOICE_ID
                left join (select INVOICE_NUMBER, SUM(QUANTITY*REDEMPTION_FEE) AS REDEMPTION_FEE,
                            MIN(ORDER_DAY) AS MIN_COST_DATE ,MAX(ORDER_DAY) AS MAX_COST_DATE
                            from  """ + db_if['rdt_o_mt']['full'] + """
                            GROUP BY INVOICE_NUMBER) red
                            on red.INVOICE_NUMBER =  ovr.INVOICE_ID
                left join (select INVOICE_NUMBER, SUM(CSF_COST) AS CHANNEL_SELLING_FEE,
                            MIN(RECEIVE_DATE) AS MIN_COST_DATE ,MAX(RECEIVE_DATE) AS MAX_COST_DATE
                            from  """ + db_if['csf_mt']['full'] + """
                            GROUP BY INVOICE_NUMBER) csf
                            on csf.INVOICE_NUMBER =  ovr.INVOICE_ID 
                WHERE INVOICE_ID NOT IN (SELECT INVOICE_NUMBER 
                                         FROM """ + db_if['coop_vali']['full'] + """ )
            """
        ,
        """update """ + db_if['coop_vali']['full'] + """  
                set total_detail_fee = (nvl(CLIP_FEE,0) + nvl(PROMOTION_COST,0) + nvl(CHANNEL_SELLING_FEE,0))
            """
        ,
        """
            update """ + db_if['coop_vali']['full'] + """  
                set DIFF_OVR_DET = ROUND(ORIGINAL_BALANCE-TOTAL_DETAIL_FEE,0)
            """
        ,
        """MERGE INTO """ + db_if['coop_vali']['full'] + """  des
                USING """ + db_if['ovr_mt']['full'] + """ ovr
                ON (des.INVOICE_NUMBER = ovr.INVOICE_ID)
                WHEN MATCHED THEN
                UPDATE SET des.type_info = ovr.type_info, des.fee_id = ovr.fee_id"""
    ]
    for sql in sqls_type:
        query.run_sql(biicnn, sql)
    for sql in sqls:
        query.run_sql(biicnn, sql)

# run_table_validation()

# db_if = o_spt.db_info_new()
# # fd_cf = o_spt.folder_config()
#
# # Select tatble
# # table_name = db_if['dt_tb']['full']
# # select table
# df = query.query(biicnn, 'select * from ' + db_if['dt_tb']['full'] + " where STORAGE_REQUIREMENT = 1")
#
# type_info = {}
# for index, row in df.iterrows():
#     type_code = row['TYPE_CODE']
#     type_info[type_code] = {}
#     type_info[type_code]['TRANSIT_REQUIREMENT'] = row['TRANSIT_REQUIREMENT']
#     type_info[type_code]['NOT_NULL_COL'] = row['NOT_NULL_COL']
#     type_info[type_code]['TRANS_TABLE'] = row['TRANS_TABLE']
#     type_info[type_code]['TRANS_DB'] = s_spt.get_db_name(row['TRANS_TABLE'])
#     type_info[type_code]['DEST_TABLE'] = row['DEST_TABLE']
#     if type_info[type_code]['DEST_TABLE'] == None:
#         type_info[type_code]['DEST_DB'] = None
#     else:
#         type_info[type_code]['DEST_DB'] = s_spt.get_db_name(row['DEST_TABLE'])
#
#     # get list cols of table:
#     df_cols = query.query(biicnn,
#                           """SELECT  table_name, column_name, data_type
#                           FROM all_tab_columns
#                           where table_name = '""" + type_info[type_code]['TRANS_TABLE'] + "' and owner = '" +
#                           type_info[type_code]['TRANS_DB'] + "'")
#
#
#
# man_cols_list = ['ID', 'REQ_ID', 'WITHDRAW_TIME', 'INVOICE_NUMBER']
# type_info = s_spt.get_list_ori_cols(man_cols_list)
# print(type_info)
# db_if = o_spt.db_info_new()
# df = query.query(biicnn, 'select * from ' + db_if['dt_tb']['full'] + " where STORAGE_REQUIREMENT = 1")
#
# type_info = {}
# for index, row in df.iterrows():
#     type_code = row['TYPE_CODE']
#     type_info[type_code] = {}
#     type_info[type_code]['TRANSIT_REQUIREMENT'] = row['TRANSIT_REQUIREMENT']
#     type_info[type_code]['NOT_NULL_COL'] = row['NOT_NULL_COL']
#     type_info[type_code]['TRANS_TABLE'] = row['TRANS_TABLE']
#     type_info[type_code]['TRANS_DB'] = s_spt.get_db_name(row['TRANS_TABLE'])
#     type_info[type_code]['DEST_TABLE'] = row['DEST_TABLE']
#     if type_info[type_code]['DEST_TABLE'] == None:
#         type_info[type_code]['DEST_DB'] = None
#     else:
#         type_info[type_code]['DEST_DB'] = s_spt.get_db_name(row['DEST_TABLE'])
#
#     # get list cols of table:
#     df_cols = query.query(biicnn,
#                           """SELECT  table_name, column_name, data_type
#                           FROM all_tab_columns
#                           where table_name = '""" + type_info[type_code]['TRANS_TABLE'] + "' and owner = '" +
#                           type_info[type_code]['TRANS_DB'] + "'")
#
#     list_cols = []
#     for col in df_cols['COLUMN_NAME']:
#         if col not in man_cols_list:
#             list_cols = list_cols + [col]
#
#     type_info[type_code]['LIST_COLS'] = list_cols
