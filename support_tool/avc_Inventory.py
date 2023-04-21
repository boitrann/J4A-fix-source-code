# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 15:55:29 2020

@author: NGUYEN Y
"""

from time import sleep
from login_page import  login_amz_vendor_central as lg
import datetime
def inventory_download(driver):
    driver.implicitly_wait(5)
    driver.find_element_by_xpath('//*[@id="downloadButton"]/awsui-button-dropdown/div/awsui-button/button/span').click()
    select_status = 'Not Yet'
    for i in range(1,4):
        driver.implicitly_wait(5)
        ele = driver.find_element_by_xpath('//*[@id="downloadButton"]/awsui-button-dropdown/div/div/ul/li[' + str(i) + ']/p')
        #print(ele.text)
        if ele.text == 'Inventory Health':
            for j in range(1,3):
                driver.implicitly_wait(5)
                ele_2 = driver.find_element_by_xpath('//*[@id="downloadButton"]/awsui-button-dropdown/div/div/ul/li[' + str(i)  + ']/ul/li[' + str(j) + ']')
                if ele_2.text == 'As Excel Workbook (.xlsx)':                               
                    ele_2.click()
                    select_status = 'Done'
                    break
        if select_status == 'Done':
            break

def inventory_filter(driver, tg_start_date_str, tg_end_date_str, distributor_view):    
    # Get in Page:
    driver.get('https://vendorcentral.amazon.com/analytics/dashboard/inventoryHealth')
    sleep(5)
    
    # Filter Distributor view
    ele = None
    success_filter = 'Fail'    
    while success_filter == 'Fail':
        driver.implicitly_wait(5)
        # distributor view
        driver.find_element_by_xpath('//*[@id="dashboard-filter-distributorView"]/div/awsui-button-dropdown/div/awsui-button/button/span/span/span').click()
        for i in range(1,3):
            driver.implicitly_wait(5)
            ele = driver.find_element_by_xpath('//*[@id="dashboard-filter-distributorView"]/div/awsui-button-dropdown/div/div/ul/li[' + str(i) + ']')
            #print(ele.text)
            if ele.text == distributor_view or ele.text == ('• ' + distributor_view):
                ele.click()
                success_filter = 'Success'
                break
    ele = None

    # Filter Reporting range
    reporting_range = 'Daily'
    success_filter = 'Fail'
    # range daily filter
    while success_filter == 'Fail':
        driver.implicitly_wait(5)
        driver.find_element_by_xpath('//*[@id="dashboard-filter-reportingRange"]/div/awsui-button-dropdown/div/awsui-button/button/span/span/span').click()
        for i in range(1,6):
            driver.implicitly_wait(5)
            ele = driver.find_element_by_xpath('//*[@id="dashboard-filter-reportingRange"]/div/awsui-button-dropdown/div/div/ul/li[' + str(i) + ']')
            #print(ele.text)
            if ele.text == reporting_range or ele.text == ('• ' + reporting_range):
                ele.click()
                success_filter = 'Success'
                break
    ele = None
    
    # Filter Date range:
    # FILTER AS SELECTED DATE
    import dateparser
    from datetime import datetime
    # input target date:
    tg_start_date_dt = dateparser.parse(tg_start_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
    tg_start_my_str = tg_start_date_dt.strftime('%B %Y')
    tg_start_my_dt = dateparser.parse(tg_start_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
    tg_start_day = int(tg_start_date_dt.strftime('%d'))
    
    tg_end_date_dt = dateparser.parse(tg_end_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
    tg_end_my_str = tg_end_date_dt.strftime('%B %Y')
    tg_end_my_dt = dateparser.parse(tg_end_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
    tg_end_day = int(tg_end_date_dt.strftime('%d'))
    #start date filter
    driver.find_element_by_xpath('//*[@id="dashboard-filter-periodPicker"]/div/div/div[1]/input').click()
                           
    web_start_my_str = ''
    while tg_start_my_str != web_start_my_str:
        driver.implicitly_wait(5)
        # current month - year
        web_start_my_str = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[1]/div[1]').text
        web_start_my_dt = dateparser.parse(web_start_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #web_start_my_dt = date_time_obj.strftime('%B %Y')
        #print("current Web my: " + web_start_my_str)
        if tg_start_my_dt > web_start_my_dt:
            driver.implicitly_wait(5)
            next_month_ele = driver.find_element_by_xpath('/html/body/div[4]/div/a[2]')
            #print('Next Month')
            next_month_ele.click()
        elif tg_start_my_dt < web_start_my_dt:
            #print('Previous Month')
            driver.implicitly_wait(5)
            previ_month_ele = driver.find_element_by_xpath('/html/body/div[4]/div/a[1]')
            previ_month_ele.click()
    
    found_status = 'Not Yet'
    no_week =  len(driver.find_elements_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div/div[1]'))
    for week in range(no_week):
        for day in range(7):
            #Check same day:
            # nếu ngày bắt đầu bé hơn 20 thì lấy từ đầu đến nay
            # ngược lại nếu ngày bắt đầu lớn hơn 20 thì lấy từ cuối tháng về
            if int(tg_start_day) <20:
                web_day_ele = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div[' + str(week + 1) + ']/div[' + str(day + 1) + ']')                               
            else:
                web_day_ele = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div[' + str(no_week-week) + ']/div[' + str(7-day) + ']')                               
            #print('Week: ' + str(week +1) + ' - Day: ' + str(day +1) + ' - Onweb: ' + str(web_day_ele.text))
            if web_day_ele.text == str(tg_start_day):
                web_day_ele.click()
                found_status = 'Found'
                break
        if found_status == 'Found':
            break
        
    sleep(1)
    # chọn ngày kết thúc
    driver.find_element_by_xpath('//*[@id="dashboard-filter-periodPicker"]/div/div/div[3]/input').click()
    web_end_my_str = ''
    while tg_end_my_str != web_end_my_str:
        driver.implicitly_wait(5)
        web_end_my_str = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[1]/div[1]').text
        web_end_my_dt = dateparser.parse(web_end_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #web_start_my_dt = date_time_obj.strftime('%B %Y')
        #print("current Web my: " + web_start_my_str)
        if tg_end_my_dt > web_end_my_dt:
            driver.implicitly_wait(5)
            next_month_ele = driver.find_element_by_xpath('/html/body/div[4]/div/a[2]')
            #print('Next Month')
            next_month_ele.click()
        elif tg_end_my_dt < web_end_my_dt:
            #print('Previous Month')
            driver.implicitly_wait(5)
            previ_month_ele = driver.find_element_by_xpath('/html/body/div[4]/div/a[1]')
            previ_month_ele.click()
    
    no_week =  len(driver.find_elements_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div/div[1]'))
    found_status = 'Not Yet'
    for week in range(no_week):
        for day in range(7):
            #Check same day:
            if int(tg_end_day) <20:
                web_day_ele = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div[' + str(week + 1) + ']/div[' + str(day + 1) + ']')
            else:
                web_day_ele = driver.find_element_by_xpath('/html/body/div[4]/div/div[2]/div[2]/div[' + str(no_week- week) + ']/div[' + str(7- day) + ']')
            #print('Week: ' + str(week +1) + ' - Day: ' + str(day +1) + ' - Onweb: ' + str(web_day_ele.text))
            if web_day_ele.text == str(tg_end_day):
                web_day_ele.click()
                found_status = 'Found'
                break
        if found_status == 'Found':
            break
    # apply-
    driver.implicitly_wait(5)
    driver.find_element_by_xpath('//*[@id="dashboard-filter-applyCancel"]/div/awsui-button[2]/button').click()   
    sleep(15)     
    
def clean_folder(folder_path):
    import os
    detail_file_names = os.listdir(folder_path)
    for detail_file_name in detail_file_names:
        os.remove(os.path.join(folder_path, detail_file_name))
def change_file_name(Initial_path,new_file_name,Target_path, file_type):
    import os
    import shutil
    from time import sleep
    filename = max([Initial_path + "\\" + f for f in os.listdir(Initial_path) if f.endswith(file_type)],key=os.path.getctime)
    sleep(2)
    #print(filename)
    #new_file_name =  new_file_name +'.' + file_type
    shutil.move(filename,os.path.join(Target_path,new_file_name))
def collect_avc_inventory(driver, tg_date_str, distributor_view, temp_dl_path, tg_dl_path):
    import dateparser
    # from support_tool import  avc
    # from support_tool  import avc_Inventory as inv_spt
    # # from support_tool import avc_Inventory as inv_spt
    # from support_tool import support_tool as spt
    import datetime
    # input date range
    datetime_str =  dateparser.parse(tg_date_str, date_formats=['%m/%d/%Y']).strftime('%Y%m%d')  # convert to GMT datetime object
    
    #Filter:
    inventory_filter(driver, tg_date_str, tg_date_str, distributor_view)
    
    # Clean temp download folder
    clean_folder(temp_dl_path)
    
    # Select Download
    withdraw_time = datetime.datetime.now()
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=120)
    download_select_status = 'Not Yet'
    while datetime.datetime.now() <= check_time:
        try:
            inventory_download(driver)
            download_select_status = 'Selected'
            #print('done select')
            break
        except:
            pass
    
    # Accept Leave notification
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=180)
    accept_status = 'Not Yet'
    if download_select_status == 'Selected':
        while datetime.datetime.now() <= check_time:
            try:
                driver.switch_to.alert.accept()
                #print('Done accept')
                accept_status = 'Accepted'
                break
            except:
                pass
    else:
        accept_status = 'Not Yet'
    
    # Move file to collected folder
    file_name = ''
    if  accept_status == 'Accepted':
        check_time = datetime.datetime.now()  + datetime.timedelta(seconds=120)
        while datetime.datetime.now() <= check_time:
            try: 
                file_name = distributor_view + '_' + datetime_str + ".xlsx"
                # spt.change_file_name(temp_dl_path, file_name,tg_dl_path,'.xlsx')
                change_file_name(temp_dl_path, file_name,tg_dl_path,'.xlsx')

                download_status = 'Downloaded'
                #current_time = datetime.datetime.now()
                #print(f'{current_time} : {download_status}')
                break
            except Exception:
                download_status = 'Not Yet'
                #current_time = datetime.datetime.now()
                #print(f'\r{current_time} : {download_status}', end = '\r')    
                pass
            sleep(1)
    else:
        download_status = 'Not Yet'
        
    return download_status, file_name, withdraw_time   


def select_bot(default_bot):
    autbot_profile = [{'Name':'Bot 1', 
                       'Temp Download path': r'D:\Download Management\AVC_Inventory\Temp Download\Bot 1' ,
                      'chrome_profile_folder':r'D:\Chrome Driver\AVC_Inventory_Profile\Bot 1'
                      },
                      
                      {'Name':'Bot 2', 
                       'Temp Download path': r'D:\Download Management\AVC_Inventory\Temp Download\Bot 2',
                       'chrome_profile_folder':r'D:\Chrome Driver\AVC_Inventory_Profile\Bot 2'}
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
    #chrome_profile_folder =  autbot_profile[bot_no - 1]['chrome_profile_folder']
    #bot_name =  autbot_profile[bot_no - 1]['Name']
    #temp_dl_path =  autbot_profile[bot_no - 1]['Temp Download path']
    return autbot_profile[bot_no - 1]

def select_function(default_function):
    function_profile = [
                            {'dist_view': 'Sourcing', 
                             'batch_man_file': '1_batchman_Sourcing',
                             'Inventory_master': '2_AvcInventory_Master'
                            }
                        ,
                      
                            {'dist_view': 'Manufacturing', 
                            'batch_man_file': '1_batchman_Manufacturing',
                             'Inventory_master': '2_AvcInventory_Master'
                            }
                      ]
    try:
        function_no = int(default_function)
    except:
        function_no = ''
    while function_no =='':
        for i in range(2):
            print(f'{i+1} - {default_function[i]["function"]}')
        # select autobot_profile to run
        print('\nVui lòng chọn Autobot thực hiện: (input số thứ tự) ')
        function_no = input()
        try:
            function_no = int(function_no)
        except:
            function_no = ''
    #chrome_profile_folder =  autbot_profile[bot_no - 1]['chrome_profile_folder']
    #bot_name =  autbot_profile[bot_no - 1]['Name']
    #temp_dl_path =  autbot_profile[bot_no - 1]['Temp Download path']
    return function_profile[function_no - 1]
# select_function(1.5)
# driver=lg.login_with_cookies('D:\AMZ_COLLECT_DATA_temp')
# import dateparser
# tg_date_dt='29/9/2021'
# tg_end_date_dt = dateparser.parse(tg_date_dt, date_formats=['%m/%d/%Y'])
# tg_date_str =  tg_end_date_dt.strftime('%m/%d/%Y')
# download_status, file_name, withdraw_time = collect_avc_inventory(driver, tg_date_str,
#                                                                           'Sourcing',
#                                                                           'D:\AMZ_COLLECT_DATA_temp',
#                                                                           'D:\AMZ_COLLECT_DATA')

