# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 16:24:20 2020

@author: Guest_1
"""


# Import libraries
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import *

from time import sleep
import datetime

import pandas as pd
import numpy as np
import os

# Chuẩn bị tool change file name
def change_file_name(Initial_path,new_file_name,Target_path, file_type):
    import os
    import shutil
    from time import sleep
    filename = max([Initial_path + "\\" + f for f in os.listdir(Initial_path) if f.endswith(file_type)],key=os.path.getctime)
    sleep(2)
    #print(filename)
    #new_file_name =  new_file_name +'.' + file_type
    shutil.move(filename,os.path.join(Target_path,new_file_name))

# Write df to excel
def write_excel(data,file_name):
    import pandas as pd
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(file_name + '.xlsx', engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    data.to_excel(writer, sheet_name=file_name, index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


# Convert text COLUMN to number COLUMN in merge file
def convert_text_to_number_2(df_full,df_column,to_type):
    import pandas as pd
    import numpy as np
    try:
        df_full[df_column] = df_full[df_column].str.replace('$','')
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace(",","")
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace("-","")
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace(" ","")
    except:
        pass
    try:
        df_full.loc[df_full[df_full[df_column] == ''].index,df_column] = 0
    except:
        pass
    try:
        df_full[df_column]  = pd.to_numeric(df_full[df_column] , downcast=to_type) # Type: float, int
    except:
        pass
    df_full[df_column] = df_full[df_column].astype(to_type)
    df_full[df_column] = df_full[df_column].replace(np.nan,0)
    
def date_timepoint(input_date, timepoint):
    output_date = ''
    if timepoint == 'beginning':
        output_date = datetime.datetime(input_date.year, input_date.month, input_date.day, 0,0,0,000000)
    if timepoint == 'ending':
        output_date = datetime.datetime(input_date.year, input_date.month, input_date.day, 23,59,59,999999)
    return output_date

def text_to_time_gmt(str_datetime):
    import dateparser
    # from string like "29/05/2020, 08:18 WIB" to GMT yyyymmddhhmmss
    date_time_obj = dateparser.parse(str_datetime, date_formats=['%B %d, %Y, %I:%M:%S %p %Z'], 
    settings={'TO_TIMEZONE': 'GMT'})  # convert to GMT datetime object
    return date_time_obj.replace(tzinfo = None)

def text_to_time_pt(str_datetime):
    import dateparser
    import pytz
    pt = pytz.timezone('US/Pacific')
    # from string like "29/05/2020, 08:18 WIB" to GMT yyyymmddhhmmss
    date_time_obj = dateparser.parse(str_datetime, date_formats=['%B %d, %Y, %I:%M:%S %p %Z'], 
    settings={'TO_TIMEZONE': 'GMT'})  # convert to GMT datetime object
    date_time_obj = date_time_obj.astimezone(pt)
    return date_time_obj.replace(tzinfo = None)

def text_to_time_pt_2(str_datetime):
    import dateparser
    import pytz
    pt = pytz.timezone('US/Pacific')
    # from string like "Thursday, January 14, 2021 8:54 AM +07" to GMT yyyymmddhhmmss
    #utc = pytz.utc
    str_datetime_notz = str_datetime[0:str_datetime.find(' +')]
    timezone_offset = int(str_datetime[str_datetime.find(' +') + 2:len(str_datetime)])
    date_time_obj = dateparser.parse(str_datetime_notz, date_formats=['%A, %B %d, %Y %I:%M %p'])  # convert to GMT datetime object
    date_time_obj = date_time_obj + datetime.timedelta(seconds = 60*timezone_offset)   
    
    
    date_time_obj = date_time_obj.astimezone(pt)
    return date_time_obj.replace(tzinfo = None)

def time_gmt_format(str_datetime):
    import dateparser
    # from string like "29/05/2020, 08:18 WIB" to GMT yyyymmddhhmmss
    date_time_obj = dateparser.parse(str_datetime, date_formats=['%B %d, %Y, %I:%M:%S %p %Z'], 
    settings={'TO_TIMEZONE': 'GMT'})  # convert to GMT datetime object
    return date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

def time_pst_format(str_datetime):
    import dateparser
    # from string like "29/05/2020, 08:18 WIB" to GMT yyyymmddhhmmss
    date_time_obj = dateparser.parse(str_datetime, date_formats=['%B %d, %Y, %I:%M:%S %p %Z'], 
    settings={'TO_TIMEZONE': 'PST'})  # convert to GMT datetime object
    return date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

def time_pt_format(str_datetime):
    import dateparser
    # from string like "29/05/2020, 08:18 WIB" to GMT yyyymmddhhmmss
    date_time_obj = dateparser.parse(str_datetime, date_formats=['%B %d, %Y, %I:%M:%S %p %Z'], 
    settings={'TO_TIMEZONE': 'PT'})  # convert to GMT datetime object
    return date_time_obj.strftime('%Y-%m-%d %H:%M:%S')


# Convert text COLUMN to DATETIME COLUMN in merge file
def convert_text_to_date(df_full,df_column):    
    for cell in range(len(df_full)):
        print(str(cell) + ' / ' +str(len(df_full)))
        try:
            df_full[df_column][cell]= time_gmt_format(df_full[df_column][cell])
        except:
            pass
    df_full[df_column] = df_full[df_column].astype('datetime64[ns]')
    
# DROPSHIP FILTER TOOL
def amzvd_dsorder_filter(driver, tg_start_date_str, tg_end_date_str,status_select):
    # FILTER AS STATUS:
    status_filter = {'NEW': False, 'ACCEPTED':False, 'SHIPPED':False, 'CANCELLED':False}
    #[k for k in status_filter.keys()]
    for key in status_filter.keys():
        for status_select_loop in status_select:
            if status_select_loop == key:
                status_filter[key] = True
    url_link = driver.current_url 
    check_text = ''
    #filter_para = ''
    for key in status_filter.keys():
        check_text = 'statuses=' + key
        if url_link.find(check_text) < 0 and status_filter[key] == True:
            url_link = url_link + "&" + check_text
            #print('Web status: ' + status_select_loop + ' Active')
        if url_link.find(check_text) > 0 and status_filter[key] == False:
            url_link = url_link.replace(check_text,'')
    
    driver.get(url_link)     
    
    sleep(2)
    
    # FILTER AS SELECTED DATE
    import dateparser
    from datetime import datetime
    limit_days = 60
    num_days = limit_days + 1
    # input target date:
    while num_days >limit_days or num_days < 0:
        if tg_start_date_str == "Input":
            print('\nVui lòng nhập ngày bắt đầu: ')
            tg_start_date_str = input()
        tg_start_date_dt = dateparser.parse(tg_start_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
        tg_start_my_str = tg_start_date_dt.strftime('%B %Y')
        tg_start_my_dt = dateparser.parse(tg_start_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        tg_start_day = int(tg_start_date_dt.strftime('%d'))
    
        if tg_end_date_str == "Input":
            print('\nVui lòng nhập ngày kết thúc: ')
            tg_end_date_str = input()
        tg_end_date_dt = dateparser.parse(tg_end_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
        tg_end_my_str = tg_end_date_dt.strftime('%B %Y')
        tg_end_my_dt = dateparser.parse(tg_end_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        tg_end_day = int(tg_end_date_dt.strftime('%d'))
        
        delta = tg_end_date_dt - tg_start_date_dt
        num_days = delta.days + 1
        print('\nlength of Period is: ' + str(num_days) + ' days')
        if num_days > limit_days or num_days <0:
            print('\nLenght of period must be from min 1 day to max '+ str(limit_days) + ' days')
            tg_start_date_str = "Input"
            tg_end_date_str = "Input"
    
        
    # select start date in calendar web
    #driver.switch_to.window(driver.window_handles[0])
    web_start_my_str = ''
    try:
        driver.implicitly_wait(2)
        web_start_my_str = driver.find_element_by_xpath('//*[@id="a-popover-content-1"]/div/div/div/div[2]').text
    except:
        pass
    # select start date in calendar web
    if web_start_my_str == '':
        search_startdate_xpath = '//*[@id="order-search-view"]/div/div[3]/div/div/div[1]/div[3]/div/div[2]/ul/li/span/div[1]/div/div[2]/div/div/div/span/i'
        driver.implicitly_wait(3)
        driver.find_element_by_xpath(search_startdate_xpath).click()

    
    while tg_start_my_str != web_start_my_str:
        web_start_my_str = driver.find_element_by_xpath('//*[@id="a-popover-content-1"]/div/div/div/div[2]').text
        web_start_my_dt = dateparser.parse(web_start_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #web_start_my_dt = date_time_obj.strftime('%B %Y')
        #print("current Web my: " + web_start_my_str)
        if tg_start_my_dt > web_start_my_dt:
            next_month_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-1"]/div/div/div/div[3]/a')
            #print('Next Month')
            next_month_ele.click()
        elif tg_start_my_dt < web_start_my_dt:
            #print('Previous Month')
            previ_month_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-1"]/div/div/div/div[1]/a')
            previ_month_ele.click()
    
    found_status = 'Not Yet'
    for week in range(6):
        for day in range(7):
            #Check same day:
            web_day_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-1"]/div/div/table/tbody/tr[' + str(week +1)+ ']/td[' + str(day + 1) + ']')
            #print('Week: ' + str(week +1) + ' - Day: ' + str(day +1) + ' - Onweb: ' + str(web_day_ele.text))
            if web_day_ele.text == str(tg_start_day):
                web_day_ele.click()
                found_status = 'Found'
                break
        if found_status == 'Found':
            break
    sleep(1)            
    # select end date in calendar web
    
    web_end_my_str = ''
    try:
        driver.implicitly_wait(1)
        web_end_my_str = driver.find_element_by_xpath('//*[@id="a-popover-content-2"]/div/div/div/div[2]').text
    except:
        pass
    # select start date in calendar web
    if web_end_my_str == '':
        search_enddate_xpath = '//*[@id="order-search-view"]/div/div[3]/div/div/div[1]/div[3]/div/div[2]/ul/li/span/div[2]/div/div[2]/div/div/div/span/i'
        driver.implicitly_wait(3)
        driver.find_element_by_xpath(search_enddate_xpath).click()
    
    while tg_end_my_str != web_end_my_str:
        web_end_my_str = driver.find_element_by_xpath('//*[@id="a-popover-content-2"]/div/div/div/div[2]').text
        web_end_my_dt = dateparser.parse(web_end_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #web_start_my_dt = date_time_obj.strftime('%B %Y')
        #print("current Web my: " + web_end_my_str)
        if tg_end_my_dt > web_end_my_dt:
            #print('Next Month')
            next_month_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-2"]/div/div/div/div[3]/a')
            next_month_ele.click()
        elif tg_end_my_dt < web_end_my_dt:
            #print('Previous Month')
            previ_month_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-2"]/div/div/div/div[1]/a')
            previ_month_ele.click()
    
    found_status = 'Not Yet'
    for week in range(6):
        for day in range(7):
            #Check same day:
            web_day_ele = driver.find_element_by_xpath('//*[@id="a-popover-content-2"]/div/div/table/tbody/tr[' + str(week +1)+ ']/td[' + str(day + 1) + ']')
            #print('Week: ' + str(week +1) + ' - Day: ' + str(day +1) + ' - Onweb: ' + str(web_day_ele.text))
            if web_day_ele.text == str(tg_end_day):
                web_day_ele.click()
                found_status = 'Found'
                break
        if found_status == 'Found':
            break
    sleep(5)
    return tg_start_date_str, tg_end_date_str
    print('Done Filter')

# Convert text COLUMN to number COLUMN in merge file
def convert_text_to_number(df_full,df_column,to_type):
    import pandas as pd
    try:
        df_full[df_column] = df_full[df_column].str.replace('$','')
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace(",","")
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace("-","")
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].str.replace(" ","")
    except:
        pass
    try:
        df_full.loc[df_full[df_full[df_column] == ''].index,df_column] = 0
    except:
        pass
    try:
        df_full[df_column]  = pd.to_numeric(df_full[df_column] , downcast=to_type) # Type: float, int
    except:
        pass
    try:
        df_full[df_column] = df_full[df_column].replace(np.nan,0)
    except:
        pass

def check_modify(df_mer, df_new, keycolumn):
    list_dup_key = df_mer[df_mer[keycolumn].duplicated()][keycolumn]
    list_dup_full = df_mer[df_mer.duplicated()][keycolumn]
    list_update = list_dup_key[~list_dup_key.isin(list_dup_full)]
    list_new = df_new[~df_new[keycolumn].isin(list_dup_key)][keycolumn]
    return list_dup_full, list_update, list_new

def printProgressBar(progress,decimals):
    #fill = '▓'
    fill = '*'
    length = 50
    #decimals = 2
    percent = ("{0:." + str(decimals) + "f}").format(100 * (progress))
    #percent = round(progress*100,2)
    filledLength_1 = int(min(length/2,int(progress*length)) )
    filledLength_2 = int(max(0,int(progress*length) - length/2))
    bar_half_1 = fill * filledLength_1 + '_' * int(length/2 - filledLength_1)
    bar_half_2 = fill * filledLength_2 + '_' * int(length/2 - filledLength_2)
    #bar = fill * filledLength + '-' * (length - filledLength)
    current_time = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    print(f'\r[{current_time}] |{bar_half_1} {percent}% {bar_half_2}|', end = '\r')
    # Print New Line on Complete
    if progress == 1: 
        print()

def entry_date(tg_start_date_str,tg_end_date_str,limit_days):
    import dateparser
    from datetime import datetime
    #limit_days = 85
    num_days = limit_days + 1
    # input target date:
    while num_days >limit_days or num_days < 0:
        if tg_start_date_str == "Input":
            print('Nhập ngày cần lấy dữ liệu: lưu ý tối đa ' + str(limit_days) + 'ngày')
            print('\nVui lòng nhập ngày bắt đầu: ')
            tg_start_date_str = input()
        tg_start_date_dt = dateparser.parse(tg_start_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
        #tg_start_my_str = tg_start_date_dt.strftime('%B %Y')
        #tg_start_my_dt = dateparser.parse(tg_start_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #tg_start_day = int(tg_start_date_dt.strftime('%d'))
    
        if tg_end_date_str == "Input":
            print('\nVui lòng nhập ngày kết thúc: ')
            tg_end_date_str = input()
        tg_end_date_dt = dateparser.parse(tg_end_date_str, date_formats=['%m/%d/%Y'])  # convert to GMT datetime object
        #tg_end_my_str = tg_end_date_dt.strftime('%B %Y')
        #tg_end_my_dt = dateparser.parse(tg_end_my_str + ' 01', date_formats=['%B %Y %d'])  # convert to GMT datetime object
        #tg_end_day = int(tg_end_date_dt.strftime('%d'))
        
        delta = tg_end_date_dt - tg_start_date_dt
        num_days = delta.days + 1
        print('\nlength of Period is: ' + str(num_days) + ' days')
        if num_days > limit_days or num_days <0:
            print('\nLenght of period must be from min 1 day to max '+ str(limit_days) + ' days')
            tg_start_date_str = "Input"
            tg_end_date_str = "Input"
    return tg_start_date_str,tg_end_date_str

def clean_folder(folder_path):
    import os
    detail_file_names = os.listdir(folder_path)
    for detail_file_name in detail_file_names:
        os.remove(os.path.join(folder_path, detail_file_name))
    
def get_batch_id(id_column, df_batch_man):
    if len(df_batch_man)>0:
        batch_id = df_batch_man[id_column].max() + 1
    else: 
        batch_id = 1
    return batch_id

def get_new_id(id_column, df_man):
    if len(df_man)>0:
        new_id = df_man[id_column].max() + 1
    else: 
        new_id = 1
    return new_id

def generate_folder(folder_path):
    try:
        os.mkdir(folder_path)
    except:
        pass
    

def get_last_id(id_column, df_man):
    if len(df_man)>0:
        new_id = df_man[id_column].max()
    else: 
        new_id = 0
    return new_id



def date_alocate(from_datetime, to_datetime):
    import datetime
    #import pandas as pd
    a = from_datetime
    
    b = to_datetime
    
    total_len =  (b - a).days*(24*3600) + (b-a).seconds
    delta = (b - a).days + 1
    #df_date_alocate = pd.DataFrame([],columns = ['Date','Len'])
    date_alocate_full = []
    accumulate_len = 0
    
    for i in range(delta):
        process_date = b - datetime.timedelta(days=i)
        process_date = process_date.date()
        print(process_date)
        new_alocate = {}
        
        
        # process last date:
        if b.date() == process_date:
            to_time = b
            if a.date() == process_date:
                print('first_date =  last date')
                from_time = a
            else:
                print('last date')
                from_time = datetime.datetime(process_date.year,process_date.month,process_date.day,0,0,0,000000)
            new_alocate['Date'] = process_date
            new_alocate['Len'] = (to_time - from_time).days + (to_time-from_time).seconds/(3600*24)       
        else:
            if a.date() == process_date:
                print('first_date')
                new_alocate['Date'] = process_date
                new_alocate['Len'] = (total_len - accumulate_len)/(24*3600)            
            else:
                print('mid_date')
                new_alocate['Date'] = process_date
                new_alocate['Len'] = 1
        
        accumulate_len = accumulate_len + new_alocate['Len']*24*3600
        
        #df_date_alocate = df_date_alocate.append(new_alocate, ignore_index = True)                        
        date_alocate_full.append(new_alocate)
    return date_alocate_full

def remove_str_bylist(series, list_apply):
    for term in list_apply:
        try:
            series = series.str.replace(term,'')
        except:
            pass
    return series


def get_max_id_ora(table_name, id_columns):
    from support_tool.Connection import Connect_y4abii as connection
    # Get current max id from rquest man
    cnn, cursor = connection.connection()
    sql_max_id = 'select max(' + str(id_columns) + ')  as MAX from ' + table_name
    df_fullog_maxid = pd.read_sql_query(sql_max_id,cnn)
    cnn = None

    current_max_id = df_fullog_maxid.iloc[0]['MAX']
    if current_max_id is None:
        current_max_id = 0
    df_fullog_maxid = None
    
    return current_max_id
    
    
def get_columns_ora(table_name):
    import pandas as pd
    from support_tool.Connection import Connect_y4abii as connection
    sql_select = 'select * from ' + table_name + ' WHERE ROWNUM = 1'
    cnn, cursor = connection.connection()
    df = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None
    
    cols = df.columns    
    return cols

def check_modify_advance(df_mer, keycolumn, mer_columns):
    df_mer = df_mer.reset_index(drop = True)
    df_mer['ID_check'] = df_mer.index + 1## tạo thêm 1 cột id = index +1

    df_mer_new = df_mer[df_mer['Source']=='New'] # data được đọc từ filde
    df_mer_old = df_mer[df_mer['Source']=='Old'] # data được retrive từ db
    list_new = df_mer_new[~df_mer_new[keycolumn].isin(df_mer_old[keycolumn])][keycolumn] #xóa những trh đuplicate
    list_new = list_new.unique()
    
    list_dup_key_checkid = df_mer_new[df_mer_new[keycolumn].isin(df_mer_old[keycolumn])]['ID_check']
    # check những order dup so với file tải về và từ db
    # kiểm tra xem những record đó có giống nhau hay không -> remove > giữ lại những record đã update
    list_dup_full_checkid = df_mer[df_mer[mer_columns].duplicated()]['ID_check']
    list_update_checkid = list_dup_key_checkid[~list_dup_key_checkid.isin(list_dup_full_checkid)] #loại bỏ những id trùng giữ lại các id đc update
    list_update = df_mer[df_mer['ID_check'].isin(list_update_checkid)][keycolumn]
    list_update = list_update.unique()

    # Những trường hợp duplcate
    list_duplicate_temp = df_mer[df_mer['ID_check'].isin(list_dup_full_checkid)][keycolumn]
    list_duplicate = list_duplicate_temp[~list_duplicate_temp.isin(list_update)]
    list_duplicate = list_duplicate.unique()
    
    return list_duplicate, list_update, list_new







def get_full_table(ora_table):
    from support_tool.Connection import Connect_y4abii as connection
    sql_select = 'select * from ' + ora_table
    cnn, cursor = connection.connection()
    df = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None

    return df


def query_db(sql):
    from support_tool.Connection import Connect_y4abii as connection
    cnn, cursor = connection.connection()
    df = pd.read_sql_query(sql,cnn)    
    cnn = None

    return df

def upper_titile_df(df):
    new_cols = []
    for col in df.columns:
        new_cols = new_cols + [col.upper().replace(' ', '_')]
    df.columns = new_cols
    return df

def upper_df_cols(df):
    cols_new = []
    for col in df.columns:
        col_new =  col.upper()
        cols_new = cols_new + [col_new]
        
    df.columns = cols_new

def getnow(tz_i):
    return datetime.datetime.now(tz = tz_i).replace(tzinfo = None)
    
    
    
    