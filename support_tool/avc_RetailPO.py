# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:07:39 2020

@author: NGUYEN Y
"""

def declear_db_files():
    database_files = {'Batch_Man': '1_FullPOSum_Batchman',
                  'Sum_Fullog': '2_FullPOSum_Fullog',
                  'Sum_Master': '2_FullPOSum_Master',
                  'Addin_Fullog': '3_FullPOAddin_Fullog',
                  'Addin_Master': '3_FullPOAddin_Master',
                  'Detail_Fullog': '4_FullPODetail_Fullog',
                  'Detail_Master': '4_FullPODetail_Master',
                  }
    return database_files
def select_function(default_function):
    function_all = [{'Function' : 'Closed PO', 
                     'batch_man_file': 'A1_batchman_ClosedPO',
                     'sum_fullog_file': 'A2_HistorySummary_Fulllog',
                     'sum_master_file': 'A2_HistorySummary_Master',
                     'detail_fullog_file': 'A3_HistoryDetail_Fulllog',
                     'detail_master_file': 'A3_HistoryDetail_Master',
                     },
                    {'Function' : 'On going PO', 
                     'batch_man_file': 'B1_batchman_OnGoingPO',
                     'sum_fullog_file': 'B2_OnGoingSummary_Fulllog',
                     'sum_master_file': 'B2_OnGoingSummary_Master',
                     'detail_fullog_file': 'B3_OnGoingDetail_Fulllog',
                     'detail_master_file': 'B3_OnGoingDetail_Master',
                     }
                  ]
    try:
        function_no = int(default_function)
    except:
        function_no = ''
        
    while function_no =='':
        for i in range(2):
            print(f'{i+1} - {function_all[i]["Function"]}')
        # select autobot_profile to run
        print('\nVui lòng chọn Function: (input số thứ tự) ')
        function_no = input()
        try:
            function_no = int(function_no)
        except:
            function_no = ''
    
    bot_function = function_all[function_no - 1]['Function']
    batch_man_filename = function_all[function_no - 1]['batch_man_file']
    
    
    return  bot_function, batch_man_filename, function_all[function_no - 1]



def select_bot(default_bot):
    autbot_profile = [{'Name':'Retail PO Bot 1', 
                       'Temp Download path': r'D:\Download Management\AVC_RetailPO\Temp Download\Retail PO Bot 1' ,
                      'chrome_profile_folder':r'D:\Chrome Driver\AVC_RetailPO_Profile\Retail PO Bot 1'
                      },
                      
                      {'Name':'Retail PO Bot 2', 
                       'Temp Download path': r'D:\Download Management\AVC_RetailPO\Temp Download\Retail PO Bot 2',
                       'chrome_profile_folder':r'D:\Chrome Driver\AVC_RetailPO_Profile\Retail PO Bot 2'},
                      
                      {'Name':'Retail PO Bot 3', 
                       'Temp Download path': r'D:\Download Management\AVC_RetailPO\Temp Download\Retail PO Bot 3',
                       'chrome_profile_folder':r'D:\Chrome Driver\AVC_RetailPO_Profile\Retail PO Bot 3'},
                      
                      {'Name':'Retail PO Bot 4', 
                       'Temp Download path': r'D:\Download Management\AVC_RetailPO\Temp Download\Retail PO Bot 4',
                       'chrome_profile_folder':r'D:\Chrome Driver\AVC_RetailPO_Profile\Retail PO Bot 4'}
                      ]
    try:
        bot_no = int(default_bot)
    except:
        bot_no = ''
    while bot_no =='':
        for i in range(2):
            print(f'{i+1} - {autbot_profile[i]["Name"]}')
        # select autobot_profile to run
        print('\nVui lòng chọn Autobot thực hiện: (input số thứ tự) ')
        bot_no = input()
        try:
            bot_no = int(bot_no)
        except:
            bot_no = ''
    chrome_profile_folder =  autbot_profile[bot_no - 1]['chrome_profile_folder']
    bot_name =  autbot_profile[bot_no - 1]['Name']
    temp_dl_path =  autbot_profile[bot_no - 1]['Temp Download path']
    return bot_name, chrome_profile_folder, temp_dl_path

def filter_condition(input_year, input_months, auto_confirm):
    list_month = ['January', 'February', 'March', 'April', 'May', 'June', 
                  'July', 'August', 'September', 'October', 'November', 'December']
    import datetime
    selected_year = input_year
    selected_months = input_months
    
    #selected_months = 1
    #selected_year  = 'Input'
    current_year = int(datetime.datetime.now().strftime('%Y'))
    confirm = False
    while confirm == False:
        try:
            selected_year = int(selected_year)
        except:
            selected_year = 0
        # Nhập năm:
        while selected_year < (current_year - 2) or  selected_year > (current_year ):
            print('\nVui lòng nhập năm: ' + str(current_year -2) + '-' + str(current_year))
            
            selected_year = input()
            try:
                selected_year = int(selected_year)
            except:
                selected_year = 0
            if selected_year < (current_year - 2) or  selected_year > (current_year ):
                print('Vui lòng nhập lại năm cho đúng - ' + str(current_year -2) + '-' + str(current_year))
        
        
        selected_month = 1
        if len(selected_months) == 0:
            print('\nVui lòng nhập tháng: enter sau mỗi tháng chọn, kết thúc ấn 0')
            while (selected_month >=1 and selected_month <= 12):
                selected_month = input()
                try:
                    selected_month = int(selected_month)
                except:
                    selected_month = 0
                if (selected_month >=1 and selected_month <= 12):
                    selected_months.append(selected_month)
        
        selected_months_text = []
        print('\nFilter đã chọn: \n- Year: ' + str(selected_year))
        print('- Month: ')
        for s_month in selected_months:
            if (int(s_month) >=1 and int(s_month) <= 12):
                if list_month[int(s_month) - 1] not in selected_months_text:
                    print('     + ' + list_month[int(s_month) - 1])
                    selected_months_text.append(list_month[int(s_month) - 1])
        
        if auto_confirm == True:
            confirm = True
        else:
            print('Vui lòng xác nhận thông tin đã chọn:\n  1. Đồng Ý \n  2. Nhập lại ')
            confirm_text = input()
            if confirm_text == '1':
                confirm = True
            else:
                confirm = False
                selected_year = ''
                selected_months = []
    return selected_year, selected_months_text


def load_pohistory_page(driver, limit_loading_second):
    import datetime
    from login import login
    loading_status = False
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=limit_loading_second)
    while loading_status == False and datetime.datetime.now() <= check_time:
        driver.get('https://vendorcentral.amazon.com/po/vendor/members/po-mgmt/history?')
        login.check_log_out(driver)
        try:
            driver.implicitly_wait(2)
            page_content = driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[1]/div[1]/h1').text
        except:
            page_content = ''
        if page_content == 'Order history':
            loading_status = True
        else:
            loading_status = False
    return loading_status

def filter_pohistory(driver, selected_year, selected_months, limit_loading_second):
    from support_tool import avc_RetailPO as rpo_spt
    from time import sleep
    # FILTERING AS CONDITION
    # Select Year
    loaded_boolean = rpo_spt.load_pohistory_page(driver, limit_loading_second)
    if loaded_boolean == True:
        try:
            for i in range(3):
                driver.implicitly_wait(2)
                ele = driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[2]/div/div/div[1]/div[1]/div/form/div[' + str(i + 1) + ']/label')
                #print(ele.text)
                if int(ele.text) == int(selected_year):
                    ele.click()
                    ele = None
                    break
            
            # Select Months
            for i in range(4):
                for j in range(4):
                    try:
                        driver.implicitly_wait(2)
                        ele = driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[2]/div/div/div[1]/div[' + str(i + 2) + ']/div/form/div[' + str(j + 1) + ']/label')
                    except:
                        driver.implicitly_wait(2)
                        ele = driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[2]/div/div/div[1]/div[' + str(i + 2) + ']/div[2]/form/div[' + str(j + 1) + ']/label')
                    #print(ele.text)
                    driver.implicitly_wait(2)
                    ele_checkbox = ele.find_element_by_tag_name('input')
                    if ele.text in selected_months:
                        if not ele_checkbox.is_selected():
                            ele_checkbox.click()
                    else:
                        if (ele_checkbox.is_selected()):
                            ele_checkbox.click()
            select_filtered_boolean = True
        except:
            select_filtered_boolean = False
    else:
        select_filtered_boolean = False
    
    if select_filtered_boolean == True:
        try:
            driver.implicitly_wait(2)
            driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[2]/div/div/div[2]/div/kat-button/button').click()
            sleep(5)
            
            driver.implicitly_wait(2)
            number_of_result = int(driver.find_element_by_xpath('//*[@id="orderHistory"]/h1').text.replace('Search Results (','').replace(')',''))
            if number_of_result > 0:
                filtered_boolean = True
            else:
                filtered_boolean = False
        except: 
            filtered_boolean = False
    else:
        filtered_boolean = False
    
    return filtered_boolean
        
def download_history_summary(driver, temp_dl_path, target_folder_path, file_name, file_type, waiting_download_second):
    from support_tool import support_tool as spt
    import datetime
    spt.clean_folder(temp_dl_path)
    driver.implicitly_wait(5)
    driver.find_element_by_xpath('//*[@id="orderHistory"]/div/div/div/kat-dropdown-button/div/button[2]').click()
    status_download = False
    for i in range(2):
        driver.implicitly_wait(5)
        ele = driver.find_element_by_xpath('//*[@id="orderHistory"]/div/div/div/kat-dropdown-button/ul/li[' + str(i + 1) + ']')
        print(ele.text)
        if ele.text == 'Excel':
            ele.click()
            break
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=waiting_download_second)
    while datetime.datetime.now() <= check_time:
        try:
            spt.change_file_name(temp_dl_path,file_name + '.' + file_type,target_folder_path, file_type)
            status_download = True
            break
        except:
            pass
    
    return status_download

def generate_folder_FullPO(collected_path, batch_id):
    import os
    from support_tool import support_tool as spt
    tg_dl_path_sum = os.path.join(collected_path, 'Batch_' + str(batch_id),'Summary')
    tg_dl_path_detail = os.path.join(collected_path,'Batch_' + str(batch_id),'Detail')
    tg_dl_path_detail_2 = os.path.join(collected_path,'Batch_' + str(batch_id),'Detail_Confirmed')
    detail_file_prename = 'Detail_PO_'
    detail_cf_file  = 'Detail_OgPO_' + str(batch_id)
    clsummary_file_name = 'PO_HisSum'  
    cfsummary_file_name = 'PO_OgSum_Batch_' + str(batch_id)   
    
    spt.generate_folder(os.path.join(collected_path,'Batch_' + str(batch_id)))
    spt.generate_folder(os.path.join(collected_path,'Batch_' + str(batch_id),'Summary'))
    spt.generate_folder(os.path.join(collected_path,'Batch_' + str(batch_id),'Detail'))
    spt.generate_folder(os.path.join(collected_path,'Batch_' + str(batch_id),'Detail_Confirmed'))
    file_storage_config = {
                    'Summary': {'Path':tg_dl_path_sum, 
                                'Confirmed File Name': cfsummary_file_name, 
                                'Closed File Name': clsummary_file_name },
                    'Detail': {'Path':tg_dl_path_detail, 'Detail File Name': detail_file_prename, 'Path_2': tg_dl_path_detail_2, 'Detail Confirmed File Name': detail_cf_file }
                    }
    return file_storage_config


def generate_folder_closedPO(collected_path, batch_function, batch_id):
    import os
    from support_tool import support_tool as spt
    tg_dl_path_sum = os.path.join(collected_path,batch_function,'Batch_' + str(batch_id),'Summary')
    tg_dl_path_detail = os.path.join(collected_path,batch_function,'Batch_' + str(batch_id),'Detail')
    detail_file_name = 'Detail_PO_' + str(batch_id)   
    summary_file_name = 'PO_History_Summary_Batch_' + str(batch_id)   
    
    spt.generate_folder(os.path.join(collected_path,batch_function,'Batch_' + str(batch_id)))
    spt.generate_folder(os.path.join(collected_path,batch_function,'Batch_' + str(batch_id),'Summary'))
    spt.generate_folder(os.path.join(collected_path,batch_function,'Batch_' + str(batch_id),'Detail'))
    file_storage_config = {
                    'Summary': {'Path':tg_dl_path_sum, 'File Name': summary_file_name },
                    'Detail': {'Path':tg_dl_path_detail, 'File Name': detail_file_name }
                    }
    return file_storage_config
    
def history_batch_info(df_batch_man, batch_id):
    import ast 
    from support_tool import avc_RetailPO as rpo_spt
    year = int(df_batch_man[df_batch_man['Batch_ID'] == batch_id].iloc[0]['Year'])
    months = ast.literal_eval(df_batch_man[df_batch_man['Batch_ID'] == batch_id].iloc[0]['Months'])
    batch_status = df_batch_man[df_batch_man['Batch_ID'] == batch_id].iloc[0]['Status_SumDownload']
    # Filter Condition applied
    selected_year, selected_months = rpo_spt.filter_condition(year, months, auto_confirm = True)

    return batch_status, selected_year, selected_months

def download_detail_po(driver, temp_dl_path, target_folder_path, file_name, file_type, waiting_download_second):
    from support_tool import support_tool as spt
    import datetime
    spt.clean_folder(temp_dl_path)
    driver.implicitly_wait(2)
    driver.find_element_by_xpath('//*[@id="export-drop-down"]/div/button[2]/kat-icon/i').click()
    status_download = False
    for i in range(3):
        driver.implicitly_wait(2)
        ele = driver.find_element_by_xpath('//*[@id="export-drop-down"]/ul/li[' + str(i + 1) + ']')
        #print(ele.text)
        if ele.text == 'Excel':
            ele.click()
            break
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=waiting_download_second)
    while datetime.datetime.now() <= check_time:
        try:
            spt.change_file_name(temp_dl_path,file_name + '.' + file_type,target_folder_path, file_type)
            status_download = True
            break
        except:
            pass
    
    return status_download



def pageload_sum_ongoingpo(driver, waiting_seconds):
    from login import login
    import datetime
    load_ogpo_sum_status = False
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=waiting_seconds)
    while datetime.datetime.now() <= check_time and load_ogpo_sum_status == False:
        withdraw_time = datetime.datetime.now()
        driver.get('https://vendorcentral.amazon.com/po/vendor/members/po-mgmt/home?')
        login.check_log_out(driver)
        try:
            driver.implicitly_wait(10)
            number_of_result = int(driver.find_element_by_xpath('//*[@id="confirmed-po"]/h1').text.replace('Confirmed Purchase Orders (','').replace(')',''))
        except:
            number_of_result = 0
        if number_of_result >0:
            load_ogpo_sum_status = True 
            break
    ogpo_pageload_result = {'Status': load_ogpo_sum_status, 'Time': withdraw_time,'No.PO': number_of_result}
    return ogpo_pageload_result

def download_sum_ongoingpo(driver,ogpo_pageload_status, temp_dl_path, target_folder_path, file_name, file_type, waiting_seconds):
    from support_tool import support_tool as spt
    import datetime
    spt.clean_folder(temp_dl_path)
    status_download = False
    if  ogpo_pageload_status == True:
        driver.implicitly_wait(2)
        driver.find_element_by_xpath('//*[@id="confirmed-po"]/div/div/div/kat-dropdown-button/div/button[2]/kat-icon/i').click()
        for i in range(2):
            driver.implicitly_wait(2)
            ele = driver.find_element_by_xpath('//*[@id="confirmed-po"]/div/div/div/kat-dropdown-button/ul/li[' + str(i + 1) + ']')
            #print(ele.text)
            if ele.text == 'Excel':
                ele.click()
                break
        check_time = datetime.datetime.now()  + datetime.timedelta(seconds=waiting_seconds)
        while datetime.datetime.now() <= check_time:
            try:
                spt.change_file_name(temp_dl_path,file_name + '.' + file_type,target_folder_path, file_type)
                status_download = True
                break
            except:
                pass
    return status_download


def request_ongoingpo_detail(driver,og_sum_download_status):
    # Request Detail Files
    ogpo_request_detail_status = False
    if og_sum_download_status == True:
        driver.implicitly_wait(5)
        ele = driver.find_element_by_xpath('//*[@id="select-all-checkbox"]')
        if not ele.is_selected():
            ele.click()
        try:
            driver.implicitly_wait(5)
            driver.find_element_by_xpath('//*[@id="export-selected-po-btn"]').click()
            ogpo_request_detail_status = True
        except:
            ogpo_request_detail_status = False
    return ogpo_request_detail_status


def ogpo_pl_recentdownload(driver, ogpo_request_detail_status, waiting_seconds):
    from login import login
    import datetime
    load_recentdl_status = False
    if ogpo_request_detail_status == True:
        check_time = datetime.datetime.now()  + datetime.timedelta(seconds=waiting_seconds)
        while datetime.datetime.now() <= check_time and load_recentdl_status == False:
            driver.get('https://vendorcentral.amazon.com/po/vendor/members/po-mgmt/bulk')
            login.check_log_out(driver)
            try:
                driver.implicitly_wait(5)
                content_check_result = driver.find_element_by_xpath('//*[@id="root"]/div/div[1]/div/h1/a').text
            except:
                content_check_result = 0
            if content_check_result  == 'Purchase Orders':
                load_recentdl_status = True  
                driver.implicitly_wait(5)
                driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/ul/a[2]').click()
    return load_recentdl_status


def ogpo_declear_rcdl_record_check():
    rcdl_record_check = {
                           'header': {'Request ID': '', 'Downloaded on': '', 'Download type': '',
                             'Number of Purchase Orders' : '', 'Status': '', 'Notes': ''     
                             },
                           'detail': {'Request ID': '', 'Downloaded on': '', 'Download type': '',
                             'Number of Purchase Orders' : '', 'Status': '', 'Notes': ''     
                             }
                       }
    return rcdl_record_check

def ogpo_get_dtreqid(driver,ogpo_request_detail_status,ogpo_pageload_result):
    from support_tool import avc_RetailPO as rpo_spt
    import dateparser
    # Load recent download page:
    ogpo_pl_recentdownload_status = rpo_spt.ogpo_pl_recentdownload(driver, ogpo_request_detail_status, waiting_seconds = 120)
    # Define Request ID
    recent_download_row = rpo_spt.ogpo_declear_rcdl_record_check()
    request_id = '' 
    if ogpo_pl_recentdownload_status == True:
        # Get header:
        driver.implicitly_wait(5)
        eles = driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/thead/tr/th')
        for i in range(len(eles)):
            #print(str(i)  + ' - ' + eles[i].text)
            for key in recent_download_row['header']:
                if key == eles[i].text:
                    recent_download_row['header'][key] = i
        
        # Get request ID:
        driver.implicitly_wait(5)
        total_row = len(driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/tbody/tr'))
        for i in range(total_row):
            driver.implicitly_wait(5)
            eles = driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/tbody/tr[' + str(i+1) + ']/td')
            for key in recent_download_row['detail']:
                    recent_download_row['detail'][key] = eles[recent_download_row['header'][key]].text
            
            recent_download_row['detail']['Downloaded on'] = dateparser.parse(recent_download_row['detail']['Downloaded on'], date_formats=['%B %d, %Y %I:%M %p']) 
            
            delta_seconds = (max(recent_download_row['detail']['Downloaded on'], ogpo_pageload_result['Time']) - min(recent_download_row['detail']['Downloaded on'], ogpo_pageload_result['Time'])).seconds
            
            request_id = ''             
            if delta_seconds <= 900 and int(recent_download_row['detail']['Number of Purchase Orders'].replace(',','')) == ogpo_pageload_result['No.PO']:
                request_id = recent_download_row['detail']['Request ID']
                break
    if request_id == ''  :
        print('Cannot find the request ID')
    else:
        print(request_id)
    return request_id

def ogpo_get_detailfile(driver, request_id, temp_dl_path, tg_dl_path_detail, file_name):
# Check status & Download:
    print()
    from support_tool import avc_RetailPO as rpo_spt
    import datetime
    from support_tool import support_tool as spt
    recent_download_row = rpo_spt.ogpo_declear_rcdl_record_check()
    ogpo_pl_recentdownload_status = rpo_spt.ogpo_pl_recentdownload(driver, ogpo_request_detail_status = True, waiting_seconds = 120)
    spt.clean_folder(tg_dl_path_detail)
    click_detail_download = False
    status_download = False
    request_status = 'Not Found'
    if request_id != '':
        if ogpo_pl_recentdownload_status == True:
            # Get header:
            driver.implicitly_wait(5)
            eles = driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/thead/tr/th')
            for i in range(len(eles)):
                #print(str(i)  + ' - ' + eles[i].text)
                for key in recent_download_row['header']:
                    if key == eles[i].text:
                        recent_download_row['header'][key] = i
            
            driver.implicitly_wait(5)
            total_row = len(driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/tbody/tr'))
            for i in range(total_row):
                driver.implicitly_wait(5)
                eles = driver.find_elements_by_xpath('//*[@id="root"]/div/div[2]/div[4]/div/div/div/div/div/div[2]/div/div/kat-data-table/table/tbody/tr[' + str(i+1) + ']/td')
                for key in recent_download_row['detail']:
                        recent_download_row['detail'][key] = eles[recent_download_row['header'][key]].text           
                if recent_download_row['detail']['Request ID'] == request_id:
                    request_status = recent_download_row['detail']['Status']
                    if recent_download_row['detail']['Status'] == 'COMPLETED':
                        eles[recent_download_row['header']['Notes']].find_element_by_tag_name('button').click()
                        click_detail_download = True
                    break
            
            if click_detail_download ==True:
                #print('check 1')
                check_time = datetime.datetime.now()  + datetime.timedelta(seconds=600)
                while datetime.datetime.now() <= check_time:
                    try:
                        spt.change_file_name(temp_dl_path,file_name + '.' + 'xls',tg_dl_path_detail, 'xls')
                        status_download = True
                        break
                    except:
                        pass
    
    return request_status, status_download


def ogpo_update_summary(database_path, sum_new_file_path, function_info, batch_id, ogpo_pageload_result):
    import os
    import pandas as pd
    from support_tool import support_tool as spt
    import numpy as np
    # Gom File Summary
    # Open Current Fullog & Master History Summary:
    os.chdir(database_path)
    df_sum_fullog = pd.read_excel(function_info['sum_fullog_file'] + '.xlsx')
    df_sum_master = pd.read_excel(function_info['sum_master_file'] + '.xlsx')
    mer_columns =  ['PO', 'Vendor', 'Ordered On', 'Ship to location', 
                    'Window Type', 'Window Start', 'Window End', 'Total Cases', 
                    'Total Cost']
    master_columns = ['ID', 'Batch_ID', 'Created_Datetime', 'Last_Modified_Datetime', 'Update_Status', 'Last_Status',
                    'PO', 'Vendor', 'Ordered On', 'Ship to location', 
                     'Window Type', 'Window Start', 'Window End', 'Total Cases', 
                     'Total Cost']
    fullog_columns = ['ID', 'Batch_ID', 'Withdraw_time', 'Update_Status', 'Last_Status',
                    'PO', 'Vendor', 'Ordered On', 'Ship to location', 
                     'Window Type', 'Window Start', 'Window End', 'Total Cases', 
                     'Total Cost']
    
    
    
    df_sum_new = pd.read_excel(sum_new_file_path)[mer_columns]
    
    # Check Update/New/dup
    df_mer = pd.DataFrame([], columns = mer_columns)
    df_sum_master_check = df_sum_master[mer_columns]
    df_mer = df_sum_master_check.append(df_sum_new, ignore_index = True)
    list_dup_full, list_update, list_new = spt.check_modify(df_mer, df_sum_new, 'PO')
    
    df_sum_new['Update_Status'] = np.nan
    df_sum_new.loc[df_sum_new[df_sum_new['PO'].isin(list_update)].index,'Update_Status'] =  'Update'
    df_sum_new.loc[df_sum_new[df_sum_new['PO'].isin(list_dup_full)].index,'Update_Status'] =  'Duplicated'
    df_sum_new.loc[df_sum_new[df_sum_new['PO'].isin(list_new)].index,'Update_Status'] =  'New'
    df_sum_new['ID'] = df_sum_new.index + spt.get_batch_id('ID', df_sum_fullog)
    df_sum_new['Batch_ID'] = batch_id
    df_sum_new = df_sum_new.reset_index(drop=True)
    df_sum_new['Last_Status'] = df_sum_new[['PO']].merge(df_sum_master, on='PO', how='left')['Update_Status']
    df_sum_new.loc[df_sum_new[df_sum_new['PO'].isin(list_new)].index,'Last_Status'] = 'Null'
    
    print('Summary From batch ' + str(batch_id))
    print(df_sum_new.groupby(['Update_Status', 'Last_Status'])['PO'].count())
    
    # Update fullog:
    df_sum_new_to_fullog = pd.DataFrame([], columns = fullog_columns)
    df_sum_new_to_fullog = df_sum_new_to_fullog.append(df_sum_new)
    df_sum_new_to_fullog = df_sum_new_to_fullog[fullog_columns]
    df_sum_new_to_fullog['Withdraw_time']  = ogpo_pageload_result['Time']
    
    df_sum_fullog = df_sum_fullog.append(df_sum_new_to_fullog, ignore_index = True)
    
    # Update Master:
    df_sum_new_to_master = pd.DataFrame([], columns = master_columns)
    df_sum_new_to_master = df_sum_new_to_master.append(df_sum_new[df_sum_new['Update_Status'] != 'Duplicated'])
    df_sum_new_to_master = df_sum_new_to_master[master_columns]
    df_sum_new_to_master = df_sum_new_to_master.reset_index(drop=True)
    df_sum_new_to_master['Last_Modified_Datetime'] = ogpo_pageload_result['Time']
    df_sum_new_to_master['Created_Datetime'] = df_sum_new_to_master[['PO']].merge(df_sum_master, on='PO', how='left')['Created_Datetime']
    df_sum_new_to_master.loc[df_sum_new_to_master[df_sum_new_to_master['Created_Datetime'].isnull()].index,'Created_Datetime'] = ogpo_pageload_result['Time']
    #df_sum_new_to_master['Detail_Download_Status'] = 'Not Yet'
    
    df_sum_master = df_sum_master.drop(df_sum_master[df_sum_master['PO'].isin(list_update)].index)
    df_sum_master.loc[df_sum_master[~df_sum_master['PO'].isin(df_sum_new['PO'])].index,'Update_Status'] = 'Closed'
    
    df_sum_master = df_sum_master.append(df_sum_new_to_master, ignore_index = True)
    
    return df_sum_master, df_sum_fullog, list_dup_full, list_update, list_new


def combine_sum_file(folder_path, list_sum_file, batch_id):
    import os
    import pandas as pd
    # Get New Sum
    
    df_new_sum_total = pd.DataFrame([], columns = ['Batch_ID', 'Page_Stage', 'Withdraw_time', 'Update_Status', 'Update_content',
                                                    'PO' , 'Vendor' , 'Ordered On' , 'Ship to location' , 'Window Type' , 
                                                    'Window Start' , 'Window End' , 'Total Cases' , 'Total Cost'])
    os.chdir(folder_path)
    for i in range(len(list_sum_file)):
        df_new_sum = None
        df_new_sum = pd.read_excel(list_sum_file[i]['File Name'] + '.xlsx')
        df_new_sum['Batch_ID'] = batch_id
        df_new_sum['Page_Stage'] = list_sum_file[i]['Page_Stage']
        df_new_sum['Withdraw_time'] = list_sum_file[i]['Withdraw_time']
        #df_new_sum = df_new_sum[df_new_sum_total.columns]
        df_new_sum_total = df_new_sum_total.append(df_new_sum, ignore_index = True)
        df_new_sum_total = df_new_sum_total[df_new_sum_total.columns]
    list_po_dup = df_new_sum_total[df_new_sum_total['PO'].duplicated()]['PO']
    print('Duplicated: ' + str(len(list_po_dup)))
    df_new_sum_total = df_new_sum_total.drop(df_new_sum_total[(df_new_sum_total['PO'].isin(list_po_dup)) & (df_new_sum_total['Page_Stage'] == 'Closed')].index)
    
    
    return df_new_sum_total

def fullpo_updatesummary(database_path, db_files, file_storage_config, list_sum_file, batch_id, batch_info):
    from support_tool import avc_RetailPO as rpo_spt
    from support_tool import support_tool as spt
    import datetime
    import os
    import pandas as pd
    import numpy as np
    
    df_new_sum_total  = rpo_spt.combine_sum_file(file_storage_config['Summary']['Path'], list_sum_file, batch_id)
    
    # Get Status update:
    os.chdir(database_path)
    df_sum_fullog = pd.read_excel(db_files['Sum_Fullog'] + '.xlsx')
    df_sum_master = pd.read_excel(db_files['Sum_Master'] + '.xlsx')
    
    mer_columns =  ['Page_Stage',
                    'PO' , 'Vendor' , 'Ordered On' , 'Ship to location' , 'Window Type' , 
                    'Window Start' , 'Window End' , 'Total Cases' , 'Total Cost']
    
    master_columns = ['ID', 'Batch_ID', 'Created_Datetime', 'Last_Modified_Datetime', 'Update_Status', 'Update_content',
                      'Detail_Download_Status', 'Page_Stage',
                      'PO', 'Vendor', 'Ordered On', 'Ship to location', 
                      'Window Type', 'Window Start', 'Window End', 'Total Cases', 
                     'Total Cost']
    
    fullog_columns = ['ID', 'Batch_ID', 'Withdraw_time', 'Update_Status', 'Update_content', 'Page_Stage',
                    'PO', 'Vendor', 'Ordered On', 'Ship to location', 
                     'Window Type', 'Window Start', 'Window End', 'Total Cases', 
                     'Total Cost']
    
    # Check confirm to closed PO:
    old_confirmed = df_sum_master[df_sum_master['Page_Stage']=='Confirmed']['PO']
    new_closed =  df_new_sum_total[df_new_sum_total['Page_Stage']=='Closed']['PO']
    new_confirmed = df_new_sum_total[df_new_sum_total['Page_Stage']=='Confirmed']['PO']
    listnew_confirmed_selfcheck = old_confirmed[~old_confirmed.isin(new_confirmed)]
    listnew_confirmed_selfcheck =  listnew_confirmed_selfcheck[~listnew_confirmed_selfcheck.isin(new_closed)]
    df_newcf_selfcheck = df_sum_master[df_sum_master['PO'].isin(listnew_confirmed_selfcheck)][mer_columns]
    df_newcf_selfcheck['Page_Stage'] = 'Closed'
    df_newcf_selfcheck['Batch_ID'] = batch_id
    df_newcf_selfcheck['Withdraw_time'] = batch_info['ClosedPO_Withdraw_time']
    df_new_sum_total  = df_new_sum_total.append(df_newcf_selfcheck, ignore_index = True)
    
    # Check update, new, dup, lost:
    df_sum_check = df_sum_master[mer_columns]
    df_mer = df_sum_check.append(df_new_sum_total[mer_columns], ignore_index = True)
    list_dup_full, list_update, list_new = spt.check_modify(df_mer, df_new_sum_total[mer_columns], 'PO')
    
    # Update status into new total:
    df_new_sum_total['Old_Page_Stage'] = df_new_sum_total[['PO']].merge(df_sum_master, on='PO', how='left')['Page_Stage']
    df_new_sum_total.loc[df_new_sum_total[df_new_sum_total['PO'].isin(list_update)].index,'Update_Status'] =  'Update'
    df_new_sum_total.loc[df_new_sum_total[df_new_sum_total['PO'].isin(list_dup_full)].index,'Update_Status'] =  'Duplicated'
    df_new_sum_total.loc[df_new_sum_total[df_new_sum_total['PO'].isin(list_new)].index,'Update_Status'] =  'New'
    df_new_sum_total['Update_content'] = df_new_sum_total['Old_Page_Stage'] + ' -> ' + df_new_sum_total['Page_Stage']
    
    df_new_sum_total.loc[df_new_sum_total[~df_new_sum_total['PO'].isin(list_update)].index,'Update_content'] =  np.nan
    df_new_sum_total.loc[df_new_sum_total[df_new_sum_total['PO'].isin(listnew_confirmed_selfcheck)].index,'Update_content'] =  'Confirmed -> Closed: Self Check'
    
    
    df_new_sum_total.groupby('Update_Status')['Page_Stage'].count()
    df_new_sum_total = df_new_sum_total.reset_index(drop=True)
    df_new_sum_total['ID'] =  df_new_sum_total.index + spt.get_batch_id('ID', df_sum_fullog)
    
    
    
    # Update Fullog:
    df_new_sum_total = df_new_sum_total[fullog_columns]
    df_sum_fullog = df_sum_fullog.append(df_new_sum_total, ignore_index = True)
    
    # Update Master:
    df_sum_new_to_master = pd.DataFrame([], columns = master_columns)
    df_sum_new_to_master = df_sum_new_to_master.append(df_new_sum_total[df_new_sum_total['Update_Status']!='Duplicated'], ignore_index = True)
    df_sum_new_to_master['Last_Modified_Datetime'] = df_sum_new_to_master['Withdraw_time']
    df_sum_new_to_master = df_sum_new_to_master[master_columns]
    df_sum_new_to_master = df_sum_new_to_master.reset_index(drop=True)
    df_sum_new_to_master['Created_Datetime'] = df_sum_new_to_master[['PO']].merge(df_sum_master, on='PO', how='left')['Created_Datetime']
    df_sum_new_to_master.loc[df_sum_new_to_master[df_sum_new_to_master['Created_Datetime'].isnull()].index,'Created_Datetime'] = batch_info['ClosedPO_Withdraw_time']
    df_sum_new_to_master['Detail_Download_Status'] = 'Not Yet'
    
    df_sum_master = df_sum_master.drop(df_sum_master[df_sum_master['PO'].isin(list_update)].index)
    df_sum_master = df_sum_master.append(df_sum_new_to_master, ignore_index = True)
    df_sum_master.loc[df_sum_master[df_sum_master['Page_Stage']=='Confirmed'].index,'Detail_Download_Status'] = 'Not Yet'
    
    note = 'New: ' + str(len(list_new))
    note = note + '\nUpdate: ' + str(len(list_update))
    note = note + ' - Update Self detect: ' + str(len(df_new_sum_total[df_new_sum_total['Update_content']=='Confirmed -> Closed: Self Check']))
    note = note + '\nDuplicate: ' + str(len(list_dup_full))
    return df_sum_master, df_sum_fullog, note
    
def fullpo_collectdetailfiles(driver, batch_id, database_path, db_files, temp_dl_path, file_storage_config):
    import os
    import pandas as pd
    import datetime
    from login import login
    from support_tool import avc_RetailPO as rpo_spt
    from support_tool import support_tool as spt
    
    os.chdir(database_path)
    df_sum_master = pd.read_excel(db_files['Sum_Master'] + '.xlsx')
    additional_info = ['PO', 'Withdraw_time',
                       'Status', 'Payment Method', 'Payment Terms', 
                       'Purchasing entity', 'Delivery Address',
                       'Note']
    
    df_additional_info = pd.DataFrame([], columns = additional_info)
    
    
    df_not_yet = df_sum_master[df_sum_master['Detail_Download_Status'] == 'Not Yet']['PO']
    total_not_yet = len(df_not_yet)
    print(f'\n{datetime.datetime.now()} - Remain Not Yet: {str(total_not_yet)}')
    
    while total_not_yet > 0:
        for po_id in df_not_yet:
            status = df_sum_master[df_sum_master['PO'] == po_id].iloc[0]['Detail_Download_Status']
            if status == 'Not Yet':
                batch_id = df_sum_master[df_sum_master['PO'] == po_id].iloc[0]['Batch_ID']
                #file_name = 'Detail_PO_' + po_id
                #target_folder_path = os.path.join(collected_path,batch_function,'Batch_' + str(batch_id),'Detail')
                additional_info_detail = {'PO': '', 'Withdraw_time': '',
                                          'Payment Method': '', 'Payment Terms': '', 'Status': '', 'Purchasing entity': '', 
                                          'Delivery Address': '',
                                          'Note' : ''}    
                # Check fail load
                load_status = 'Fail'
                alert_wrong_po = ''
                
                while load_status == 'Fail':
                    try:
                        withdraw_time = datetime.datetime.now()
                        driver.get('https://vendorcentral.amazon.com/po/vendor/members/po-mgmt/order?poId=' + po_id)
                        login.check_log_out(driver)
                        try:
                            driver.implicitly_wait(1)
                            alert_wrong_po = driver.find_element_by_xpath('//*[@id="root"]/div/div[2]').text
                        except:
                            pass
                        if alert_wrong_po == 'Could not load purchase order because it could not be found or the PO ID is not defined in the URL, for example .../order?poId=ABCD1234':
                            load_status = 'Fail -  PO is not available'
                        else:
                            driver.implicitly_wait(5)
                            number_of_result = int(driver.find_element_by_xpath('//*[@id="purchaseOrder"]/h1').text.replace('PO Items (','').replace(')',''))
                            if number_of_result >0:
                                load_status = 'Success'            
                    except:
                        load_status = 'Fail'
                
                
                detail_download_status = ''
                if load_status == 'Success':
                    driver.implicitly_wait(5)
                    test = driver.find_elements_by_xpath('//*[@id="po-header"]/kat-table/kat-table-body/kat-table-row/kat-table-cell')
                    for i in range(len(test)):
                        for key in additional_info_detail.keys():
                            if test[i].text == key:
                                additional_info_detail[key] = test[i + 1].text
                    driver.implicitly_wait(5)
                    additional_info_detail['Delivery Address'] = driver.find_element_by_xpath('//*[@id="fc-address"]').text
                    additional_info_detail['PO'] = po_id
                    additional_info_detail['Withdraw_time'] = withdraw_time
                    
                    file_storage_config['Detail']['Path'] = 'D:\\Download Management\\AVC_RetailPO\\Collected Files\\Batch_' + str(batch_id) + '\\Detail'
                    
                    detail_download_status = rpo_spt.download_detail_po(driver, temp_dl_path, file_storage_config['Detail']['Path'], 
                                                                        file_name = file_storage_config['Detail']['Detail File Name'] + po_id, file_type = 'xlsx', waiting_download_second = 120)
                    if detail_download_status == True:
                        df_sum_master.loc[df_sum_master[df_sum_master['PO'] == po_id].index,'Detail_Download_Status'] = 'Downloaded'
                elif load_status == 'Fail -  PO is not available':
                    additional_info_detail['PO'] = po_id
                    additional_info_detail['Withdraw_time'] = withdraw_time
                    additional_info_detail['Note'] = 'Not Available PO'
                    additional_info_detail['Batch_ID'] = batch_id
                    df_sum_master.loc[df_sum_master[df_sum_master['PO'] == po_id].index,'Detail_Download_Status'] = 'Not Available PO'
                
                if load_status != 'Fail':
                    #df_additional_info.drop(df_additional_info['PO'])
                    df_additional_info = df_additional_info.append(additional_info_detail,ignore_index=True)
                    
            
    
                                     
            total_not_yet_2 = len(df_sum_master[df_sum_master['Detail_Download_Status'] == 'Not Yet'])
            print(f'\r {datetime.datetime.now()} - Completed {total_not_yet - total_not_yet_2} / {total_not_yet}', end = '\r')
        
        print()
        df_not_yet = df_sum_master[df_sum_master['Detail_Download_Status'] == 'Not Yet']['PO']
        total_not_yet = len(df_not_yet)
        print(f'\n {datetime.datetime.now()} - Completed a round - Remain Not Yet: {str(total_not_yet)} / {str(len(df_not_yet))}')
        
    # Update addin to df db
    os.chdir(database_path)
    df_addin_fullog = pd.read_excel(db_files['Addin_Fullog'] + '.xlsx')
    df_addin_master = pd.read_excel(db_files['Addin_Master'] + '.xlsx')
    
    # Update full log
    #df_additional_info['Batch_ID'] = batch_id
    df_additional_info = df_additional_info.reset_index(drop=True)
    df_additional_info['ID'] = df_additional_info.index + spt.get_batch_id('ID', df_addin_fullog)
    df_additional_info = df_additional_info[df_addin_fullog.columns]
    df_addin_fullog = df_addin_fullog.append(df_additional_info, ignore_index = True)
    
    # Update master addin
    
    df_additional_tomaster = pd.DataFrame([], columns = df_addin_master.columns)
    df_additional_tomaster = df_additional_tomaster.append(df_additional_info, ignore_index = True)
    df_additional_tomaster['Last_Modified_Datetime'] = df_additional_tomaster['Withdraw_time']
    #df_additional_tomaster = df_additional_tomaster[df_addin_master.columns]
    df_additional_tomaster = df_additional_tomaster.reset_index(drop=True)
    df_additional_tomaster['Last_Created_Datetime'] = df_additional_tomaster[['PO']].merge(df_addin_master, on='PO', how='left')['Created_Datetime']
    df_additional_tomaster['Created_Datetime'] = df_additional_tomaster[['Last_Created_Datetime','Withdraw_time']].min(axis = 1)
    #df_sum_new_to_master['Detail_Download_Status'] = 'Not Yet'
    df_additional_tomaster = df_additional_tomaster[df_addin_master.columns]
    
    df_addin_master = df_addin_master.drop(df_addin_master[df_addin_master['PO'].isin(df_additional_tomaster['PO'])].index)
    df_addin_master = df_addin_master.append(df_additional_tomaster, ignore_index = True)
    
    return df_sum_master, df_addin_master, df_addin_fullog

def combine_detail_file(collected_path,database_path, db_files):
    
    from support_tool import support_tool as spt
    import os
    import pandas as pd
    import datetime
    import numpy as np
    
    # Gom file detail
    os.chdir(database_path)
    df_sum_master = pd.read_excel(db_files['Sum_Master'] + '.xlsx')
    df_detail_master = pd.read_excel(db_files['Detail_Master'] +  '.xlsx')
    df_detail_fullog = pd.read_excel(db_files['Detail_Fullog'] + '.xlsx')
    
    
    df_new_detail_total = pd.DataFrame([], columns = df_detail_fullog.columns)
    
    
    
    df_notyet = df_sum_master[df_sum_master['Detail_Download_Status']=='Downloaded']
    
    total_not_yet = len(df_notyet)
    
    print(f'\n{datetime.datetime.now()} - Remain Not Yet: {str(total_not_yet)}')
    
    while total_not_yet > 0:
        for po_id in df_notyet['PO']:
            status = df_sum_master[df_sum_master['PO'] == po_id].iloc[0]['Detail_Download_Status'] 
            if status == 'Downloaded':
                df_new_detail = None
                batch_id = df_sum_master[df_sum_master['PO'] == po_id].iloc[0]['Batch_ID'] 
                detail_path = os.path.join(collected_path, 'Batch_' + str(batch_id), 'Detail')
                os.chdir(detail_path)
                file_name = 'Detail_PO_' + po_id + '.xlsx'
                df_new_detail  = pd.read_excel(file_name)
                df_new_detail['Batch_ID'] = batch_id
                df_new_detail['PO'] = po_id
                df_new_detail['ID'] = np.nan
                df_new_detail = df_new_detail[df_detail_master.columns]
                df_new_detail_total = df_new_detail_total.append(df_new_detail, ignore_index = True)
                df_sum_master.loc[df_sum_master[df_sum_master['PO'] == po_id].index,'Detail_Download_Status'] = 'Processed'
                
            total_not_yet_2 = len(df_sum_master[df_sum_master['Detail_Download_Status'] == 'Downloaded'])
            print(f'\r {datetime.datetime.now()} - Completed {total_not_yet - total_not_yet_2} / {total_not_yet}', end = '\r')
        print()
        df_notyet = df_sum_master[df_sum_master['Detail_Download_Status'] == 'Downloaded']
        total_not_yet = len(df_notyet)
        print(f'\n {datetime.datetime.now()} - Completed a round - Remain Not Yet: {str(total_not_yet)}')
        
    
    df_new_detail_total = df_new_detail_total[df_detail_master.columns]
    df_new_detail_total = df_new_detail_total.reset_index(drop=True)
    df_new_detail_total['ID'] = df_new_detail_total.index + spt.get_batch_id('ID', df_detail_fullog)
    
    # Update fullog:
    df_detail_fullog = df_detail_fullog.append(df_new_detail_total, ignore_index = True)
    
    
    # Update Master
    df_detail_master = df_detail_master.drop(df_detail_master[df_detail_master['PO'].isin(df_new_detail_total['PO'])].index)
    df_detail_master = df_detail_master.append(df_new_detail_total, ignore_index = True)
    
    
    return df_sum_master, df_detail_master, df_detail_fullog
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
