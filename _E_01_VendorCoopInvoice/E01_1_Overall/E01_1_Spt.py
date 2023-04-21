# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 15:03:43 2021

@author: Guest_1
"""

import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')


import sys
sys.path.append('D:\Vendor_data_collect')


#from selenium.common.exceptions import *
import datetime
from time import sleep
import pandas as pd
import os

from support_tool import support_tool as spt
from _E_01_VendorCoopInvoice.E01_1_Overall import E01_1_Spt as s_spt
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
from support_tool.Connection import df_to_oratb as dto_spt
from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn   


from login import openbrowser
from login import login
import numpy as np

import pytz



def scan_collectdetail_req():
    db_if = o_spt.db_info_new()
    
    req_info = {} 
    ready_to_run = False
    
    df_req_overall = query.query(biicnn, 'select * from ' + db_if['ovr_rq']['full'] + " where STATUS = 'Open'")
    

    current_open = len(df_req_overall)
    
    if current_open == 0:
        print('REQ_CollectOverall: No Available request')
        ready_to_run = False
    else:
        req_id = df_req_overall['REQ_ID'].min()
        if req_id is np.nan:
            ready_to_run = False
            print('REQ_CollectOverall: Error on Req ID format')
        else:
            req_info['REQ_ID'] = req_id
            req_info['FROM_DATE'] = df_req_overall[df_req_overall['REQ_ID']==req_id].iloc[0]['FROM_DATE']
            req_info['TO_DATE'] = df_req_overall[df_req_overall['REQ_ID']==req_id].iloc[0]['TO_DATE']
            ready_to_run = True
    return ready_to_run, req_info



# Break date duration:
def break_date_duration(tg_start_date_dt, tg_end_date_dt, max_duration):
    #import dateparser
    #import datetime
    filter_date_list = []
    #tg_start_date_dt = dateparser.parse(tg_start_date_str, date_formats=['%m/%d/%Y'])
    #tg_end_date_dt = dateparser.parse(tg_end_date_str, date_formats=['%m/%d/%Y'])
    duration = max_duration + 1 
    while duration > max_duration:
        duration = (tg_end_date_dt - tg_start_date_dt).days + 1
        if duration > max_duration:
            break_end_date  = tg_start_date_dt + datetime.timedelta(days=max_duration - 1)
            filter_date_list.append({'Start_date': tg_start_date_dt.strftime('%m/%d/%Y'),
                                     'End_date': break_end_date.strftime('%m/%d/%Y')})
            tg_start_date_dt = tg_start_date_dt + datetime.timedelta(days=max_duration)
        elif duration > 0:
            break_end_date  = tg_end_date_dt
            filter_date_list.append({'Start_date': tg_start_date_dt.strftime('%m/%d/%Y'),
                                     'End_date': break_end_date.strftime('%m/%d/%Y')})
            tg_start_date_dt = tg_start_date_dt + datetime.timedelta(days=max_duration)
    return filter_date_list


def collect_summaries(driver,temp_dl_path, filter_conditions, req_info,er_info):
    # Start to collect:
    # Login:
    er_info['PHASE_NOTE'] = '11.1'
    
    summary_files_info = []
    
    er_info['PHASE_NOTE'] = '11.2'
    count = 0
    print(f'{datetime.datetime.now()} - Start get summary')
    while count < len(filter_conditions):
        count = 0
        for i in range(len(filter_conditions)):
            try:
                er_info['PHASE_NOTE'] = '11.3'
                if filter_conditions[i]['Download_Status'] == False:
                    er_info['PHASE_NOTE'] = '11.4'
                    new_file_info = s_spt.get_summary(driver, req_info['REQ_ID'], filter_conditions[i], 
                                                               temp_dl_path)
                    er_info['PHASE_NOTE'] = '11.5'
                    if new_file_info['Status'] == True:
                        summary_files_info.append(new_file_info)
                        filter_conditions[i]['Download_Status'] = True
                        count = count +1
                    #print(f'{datetime.datetime.now()} - {count} / {str(len(filter_conditions))} - {new_file_info["Status"]} {filter_conditions[i]["Download_Status"]} - From: {filter_conditions[i]["Start_date"]} - To: {filter_conditions[i]["End_date"]}')
                else:
                    er_info['PHASE_NOTE'] = '11.6'
                    count = count + 1
            except:
                pass
            print(f'{datetime.datetime.now()} - {count} / {str(len(filter_conditions))} - {filter_conditions[i]["Download_Status"]} - From: {filter_conditions[i]["Start_date"]} - To: {filter_conditions[i]["End_date"]}')

    
    return summary_files_info,er_info

def get_summary(driver,req_id,filter_condition, temp_dl_path):
    import dateparser
    dl_sm_status = False
    
    
    fd_cf = o_spt.folder_config()
    summary_path = fd_cf['collected_path_ori']
    
    
    withdraw_time, filter_status = s_spt.date_filter(driver,filter_condition['Start_date'],filter_condition['End_date'])        
    
    if filter_status == True:
        spt.clean_folder(temp_dl_path)
        driver.implicitly_wait(5)
        export_batch_xpath = driver.find_element_by_xpath(' //*[@id="select-all"]').click()
        
        driver.implicitly_wait(5)
        export_batch_xpath = driver.find_element_by_xpath('//*[@id="a-autoid-0-announce"]/span')
        export_batch_xpath.click()
        
        for i in range(3):
            #print(i)
            driver.implicitly_wait(5)
            ele_exp = driver.find_element_by_xpath('//*[@id="cc-invoice-actions-dropdown_' + str(i) + '"]')
            content = ele_exp.text
            if content == 'Export to a spreadsheet':
               ele_exp.click()  
        
        # Đổi file name
        batch_time_str = (dateparser.parse(filter_condition['Start_date'], date_formats=['%m/%d/%Y']).strftime('%y%m%d')
                      + '_' + dateparser.parse(filter_condition['End_date'], date_formats=['%m/%d/%Y']).strftime('%y%m%d'))
        
        summary_file_name = ('REQ_' + str(req_id) + '_CI_Sum_'  + batch_time_str  + '.xls')
        check_time = datetime.datetime.now()  + datetime.timedelta(seconds=180)
        while datetime.datetime.now() <= check_time:
            try: 
                spt.change_file_name(temp_dl_path, summary_file_name, summary_path,'.xls')
                dl_sm_status = True
                break
            except:
                dl_sm_status = False
                sleep(1)
                pass
        
    if dl_sm_status == True:
        summary_file_info = {'File_name': summary_file_name, 
                             'Withdraw_time': withdraw_time, 
                             'Folder_path': summary_path,
                             'Status': dl_sm_status}
    else:
        summary_file_info = {'File_name': None, 
                             'Withdraw_time': None, 
                             'Folder_path': None,
                             'Status': dl_sm_status}        
    return summary_file_info
    


def date_filter(driver,from_date,to_date):
    status = False
    withdraw_time = None
    try:
        pt = pytz.timezone('US/Pacific')
        driver.get('https://vendorcentral.amazon.com/hz/vendor/members/coop?ref_=vc_xx_subNav')
        driver.implicitly_wait(10)
        search_frDate_xpath = driver.find_element_by_xpath('//*[@id="from-date-id"]')
        search_frDate_xpath.clear()
        search_frDate_xpath.send_keys(from_date)
        
        driver.implicitly_wait(5)
        search_toDate_xpath = driver.find_element_by_xpath('//*[@id="to-date-id"]')
        search_toDate_xpath.clear()
        search_toDate_xpath.send_keys(to_date)
        
        driver.implicitly_wait(5)
        search_buttom_xpath = driver.find_element_by_xpath('//*[@id="search-button-announce"]')
        search_buttom_xpath.click()
        withdraw_time = datetime.datetime.now(tz = pt).replace(tzinfo = None)
        sleep(7)
        status = True
    except:
        status = False
    
    return withdraw_time, status
        


def combine_summary_file(summary_files_info, path_conv, req_info):

    combine_sum_columns = ['WITHDRAW_TIME',
                           'Invoice ID', 'Invoice date', 'Agreement ID',
                           'Agreement title','Funding Type', 'Original balance']
    combine_sum_columns_2 = ['WITHDRAW_TIME',
                             'INVOICE_ID', 'INVOICE_DATE', 'AGREEMENT_ID', 'AGREEMENT_TITLE','FUNDING_TYPE', 'ORIGINAL_BALANCE' ]
    number_files = len(summary_files_info)
    df_sum_full = pd.DataFrame([], columns = combine_sum_columns)
    for i in range(number_files):
        if summary_files_info[number_files - i - 1]['Status'] == True:
            df_sum_child = pd.read_excel(summary_files_info[number_files - i - 1]['Folder_path'] + '\\' + summary_files_info[number_files - i - 1]['File_name'])
            df_sum_child['WITHDRAW_TIME'] = summary_files_info[number_files - i - 1]['Withdraw_time']
            df_sum_child = df_sum_child[combine_sum_columns]
            df_sum_child_unique = df_sum_child[~df_sum_child['Invoice ID'].isin(df_sum_full['Invoice ID'])]
            list_sum_child_dup = df_sum_child[df_sum_child['Invoice ID'].isin(df_sum_full['Invoice ID'])]['Invoice ID']
            df_sum_full = df_sum_full.append(df_sum_child_unique, ignore_index = True)
            print(summary_files_info[number_files - i - 1]['File_name'] + ': Duplicated: ' + str(len(list_sum_child_dup)))    
        else: 
            print('Miss file :' + summary_files_info[number_files - i - 1]['File_name'])


    df_sum_full = df_sum_full[combine_sum_columns]
    df_sum_full.columns = combine_sum_columns_2
    df_sum_full = s_spt.convert_format_sum(df_sum_full)    
    df_sum_full['REQ_ID'] = req_info['REQ_ID']
    converted_file_name = 'REQ_' + str(req_info['REQ_ID']) + '_CI_Sum_Full'
    os.chdir(path_conv)
    spt.write_excel(df_sum_full,converted_file_name)
    return df_sum_full, converted_file_name         



def convert_format_sum(df_data):
    date_time_cols = ['INVOICE_DATE']
    text_cols = [ 'INVOICE_ID', 'AGREEMENT_ID', 'AGREEMENT_TITLE','FUNDING_TYPE']
    int_cols = []
    float_cols = ['ORIGINAL_BALANCE']

    
    # Convert date
    #date_time_cols = ['REQ_TIME', 'FROM_DATE', 'TO_DATE', 'START_TIME', 'FINISH_TIME','WITHDRAW_TIME']
    df_data[date_time_cols] = df_data[date_time_cols].astype('datetime64[ns]')
    
    
    for col in date_time_cols:
        df_data[col] = df_data[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_data[date_time_cols] = df_data[date_time_cols].astype(str)
    df_data[date_time_cols] = df_data[date_time_cols].replace({'nan': None})
    
    
    # COVERT TEXT
    #text_cols = [ 'TYPE_1','TYPE_2', 'STATUS', 'COLLECTED_FILE_NAME', 'CONVERTED_FILE_NAME',  'REMARKS']
    df_data[text_cols] = df_data[text_cols].astype(str)
    df_data[text_cols] = df_data[text_cols].replace({'nan': None})
    
    #int_cols = ['REQ_ID']
    for col in int_cols:     
        df_data[col] = pd.to_numeric(df_data[col] , downcast='signed')
    df_data[int_cols] = df_data[int_cols].fillna(0)
    
    
    for col in float_cols:
        df_data[col] = spt.remove_str_bylist(df_data[col],['+','applicable taxes',',','$',' ','-'])   
        df_data[col] = pd.to_numeric(df_data[col] , downcast='float')
    df_data[float_cols] = df_data[float_cols].fillna(0)
    
    
    df_data = df_data.where(pd.notnull(df_data), None)

    return df_data



def process_new_sum(df_new):
    db_if = o_spt.db_info_new()
    ['WITHDRAW_TIME',
     'INVOICE_ID', 'INVOICE_DATE', 'AGREEMENT_ID',
     'AGREEMENT_TITLE','FUNDING_TYPE', 'ORIGINAL_BALANCE' ]
    

    # Get master for check duplicate, update
    df_new['INVOICE_DATE'] = df_new['INVOICE_DATE'].astype('datetime64[ns]')
    min_invoice_date = df_new['INVOICE_DATE'].min()
    min_invoice_date = min_invoice_date.strftime('%Y-%m-%d')
    
    # Open Master
    df_master = query.query(biicnn, 'select * from ' + db_if['ovr_mt']['full'] + " where INVOICE_DATE >= to_timestamp('" + min_invoice_date  + " 00:00:00', 'yyyy-mm-dd hh24:mi:ss')")

    df_new = s_spt.convert_format_sum(df_new)
    df_master = s_spt.convert_format_sum(df_master)
    mer_cols = ['INVOICE_ID', 'INVOICE_DATE', 'AGREEMENT_ID',
                'AGREEMENT_TITLE','FUNDING_TYPE', 'ORIGINAL_BALANCE']
                
    df_mer = df_master[mer_cols].append(df_new[mer_cols], ignore_index = True)
    list_dup_full, list_update, list_new = spt.check_modify(df_mer, df_new[mer_cols], 'INVOICE_ID')
    
    
    # Upddate Fullog:
    # Define status of new data
    df_new['UPDATE_STATUS'] = np.nan
    df_new.loc[df_new[df_new['INVOICE_ID'].isin(list_new)].index,'UPDATE_STATUS'] = "New"
    df_new.loc[df_new[df_new['INVOICE_ID'].isin(list_update)].index,'UPDATE_STATUS'] = "Update"
    df_new.loc[df_new[df_new['INVOICE_ID'].isin(list_dup_full)].index,'UPDATE_STATUS'] = "Duplicated"

    #define last_id
    df_new = df_new.reset_index(drop  = True)
    df_new['LAST_ID'] = df_new[['INVOICE_ID']].merge(df_master, on = 'INVOICE_ID', how = 'left')['ID']

    status_note = '[Total: ' + str(len(df_new)) + ']'
    status_note = status_note + ', [New: ' + str(len(list_new)) + ']'
    status_note = status_note + ', [Update: ' + str(len(list_update)) + ']'
    status_note = status_note + ', [Duplicated: ' + str(len(list_dup_full)) + ']'
    
    
    return df_new, status_note




def gen_request_detail(req_id):
    
    db_if = o_spt.db_info_new()
    
    pt = pytz.timezone('US/Pacific')
    
    df_new_req_detail = query.query(biicnn, """select * from """+ db_if['ovr_fl']['full']+"""
                                     where req_id = """ + str(req_id) + 
                                    """ and  UPDATE_STATUS in ('Update', 'New')""")
    # Open req file:
    # Get df of pending list
    df_req_oa_pending = query.query(biicnn, "select * from " + db_if['dt_rq']['full'] + 
                                    " where STATUS = 'Pending - Waiting for adding'  order by req_id asc")
    current_pending = len(df_req_oa_pending)
    
   
    
    # Scan any pending
    #current_pending = len(df_req_oa_pending[df_req_oa_pending['STATUS']=='Pending - Waiting for adding'])
    new_req_source = '[Req_overall_id ' +str(req_id) +  ']'
    if current_pending == 0:
        new_req_info = {}
        new_req_info['REQ_TIME'] = datetime.datetime.now(tz = pt).replace(tzinfo = None).strftime('%Y-%m-%d %H:%M:%S')
        new_req_info['REQ_SOURCE'] = new_req_source
        new_req_info['STATUS'] = 'Pending - Waiting for adding'
        
        #df_req_oa = df_req_oa.append(new_req_info, ignore_index = True)
        # insert new request
        query.json_to_db(biicnn, new_req_info, [], db_if['dt_rq']['db'], db_if['dt_rq']['tb'], False)
        
        df_req_oa_pending = query.query(biicnn, "select * from " + db_if['dt_rq']['full'] + 
                                    " where STATUS = 'Pending - Waiting for adding'  order by req_id asc")
        new_oa_req_id = int(df_req_oa_pending[df_req_oa_pending['STATUS']=='Pending - Waiting for adding']['REQ_ID'].min())
    else:
        new_oa_req_id = int(df_req_oa_pending[df_req_oa_pending['STATUS']=='Pending - Waiting for adding']['REQ_ID'].min())
        req_source = df_req_oa_pending[df_req_oa_pending['REQ_ID']==new_oa_req_id].iloc[0]['REQ_SOURCE']
        
        if new_req_source not in req_source:
            req_source_update = req_source + ', ' + new_req_source
            query.run_sql(biicnn,'Update ' + db_if['dt_rq']['full'] + 
                          """ set  REQ_SOURCE = '"""+req_source_update+"""' where REQ_ID = """+ str(new_oa_req_id))
    
    df_new_req_detail['REQ_ID'] = int(new_oa_req_id)
    df_new_req_detail['REQ_SOURCE'] = new_req_source
    df_new_req_detail['STATUS_SHORTAGE'] = 'Open'
    df_new_req_detail['STATUS_DETAIL'] = 'Open'
    
    df_booked_list = query.query(biicnn,'select INVOICE_NUMBER from ' + db_if['dt_rql']['full']  + 
                                              ' where REQ_ID = ' + str(new_oa_req_id)) 

    
    df_new_req_detail = df_new_req_detail.drop(df_new_req_detail[df_new_req_detail['INVOICE_ID'].isin(df_booked_list['INVOICE_NUMBER'])].index)
    
    
    #df_new_req_detail['ID'] = df_new_req_detail.index + int(current_max_id) + 1
    df_new_req_detail = df_new_req_detail[['REQ_ID', 'REQ_SOURCE', 'INVOICE_ID', 'STATUS_SHORTAGE', 'STATUS_DETAIL']]
    df_new_req_detail.columns = ['REQ_ID', 'REQ_SOURCE', 'INVOICE_NUMBER', 'STATUS_SHORTAGE', 'STATUS_DETAIL']
    
    query.df_to_db(biicnn, df_new_req_detail, ['ID'], db_if['dt_rql']['db'] ,  db_if['dt_rql']['tb'] , True)
    
    
        
def update_fullog_to_master(mt_db, mt_tb, fl_db, fl_tb, req_info,id_column,ignore_columns,announce):
    #master_table = table
    #fullog_table = table.replace('_MASTER','_FULLOG')
    
    # select data type from ora table
    df_cols_mt = query.query(biicnn, """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                          where table_name = '""" + mt_tb + "' and owner = '" + mt_db+"'")
    df_cols_fl = query.query(biicnn, """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                          where table_name = '""" + fl_tb + "' and owner = '" + fl_db+"'")
    
    transf_cols = []
    for col_mt in df_cols_mt['COLUMN_NAME']:
        
        for col_fl in df_cols_fl['COLUMN_NAME']:
            if col_mt not in ignore_columns and col_mt == col_fl:
                transf_cols = transf_cols + [col_mt]
    
    transf_cols_text = ','.join(transf_cols)
    
    # Delete the same invoice id in master
    query.run_sql(biicnn, "delete from " +mt_db+'.'+ mt_tb + 
                          " where " + id_column  + """ in (select distinct(""" + id_column + """) 
                                                            from """ +fl_db+'.'+ fl_tb + 
                                                            """ where UPDATE_STATUS = 'Update' 
                                                                and REQ_ID = """ + str(req_info['REQ_ID']) + ")")
   
    
    
    # RUN SQL IMPORT
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {mt_db}')
    sql_insert =( 'INSERT INTO ' +mt_db+'.'+ mt_tb +  ' (' + transf_cols_text + 
                ') select ' + transf_cols_text + ' from ' + fl_db+'.'+ fl_tb + ' where req_id = ' + str(req_info['REQ_ID']) 
                + " and UPDATE_STATUS <> 'Duplicated'")
    query.run_sql(biicnn, sql_insert)
    
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')