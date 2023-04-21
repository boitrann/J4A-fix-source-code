# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 16:41:41 2020

@author: Guest_1
"""


# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 11:56:21 2020

@author: Nguyen Y
"""
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
#from selenium.common.exceptions import Exception
import pandas as pd
import numpy as np
import os
from time import sleep
import datetime
from support_tool import support_tool as spt

def collect_summary_and_download_detail(driver):
    #SET UP BATCH
    date_range_text = 'Last 60 days'
    df_sm_batch_man = pd.read_excel(r'D:\Download Management\Download PO Invoices Detail\New Process\Database\1_dl_sm_batch_man.xlsx')
    batch_id = df_sm_batch_man['BATCH_ID'].max() + 1
    print('\nStart process for new batch: Batch '+ str(batch_id))
    
    # DOWNLOAD NEW PO INVOICE
    # Get Invoice Summary File: to get new & update invoice
    driver.switch_to.window(driver.window_handles[0])
    driver.get('https://vendorcentral.amazon.com/hz/vendor/members/inv-mgmt/search') 
    
    driver.implicitly_wait(10)
    driver.find_element_by_xpath('//*[@id="date-range-option"]/span/span/span').click()
    
    # Search by condition filtered
    for date_select in range(5):
        driver.implicitly_wait(10)
        select_date_range = driver.find_element_by_xpath('//*[@id="date-range-option_'+ str(date_select + 1) + '"]')
        if select_date_range.text == date_range_text:
            select_date_range.click()
            break
    driver.implicitly_wait(10)
    driver.find_element_by_xpath('//*[@id="advancedSearchHarmonicForm-submit"]').click()    
    
    
    #Download summary file:
    Initial_path =R"D:\Download Management\Download PO Invoices Detail\Temporary Download area"
    Target_path = R'D:\Download Management\Download PO Invoices Detail\Downloaded_Summary'
    spt.clean_folder(Initial_path)
    driver.implicitly_wait(10)
    driver.find_element_by_xpath('//*[@id="advancedSearchExportAll"]').click()    
    
    
    withdraw_time = datetime.datetime.now()
    batch_id_str = withdraw_time.strftime('%Y%m%d_%H%M%S')
    
    df_sm_batch_man.loc[len(df_sm_batch_man)] = [batch_id,withdraw_time,batch_id_str,date_range_text] #Update new batch into batch management
    
    summary_file_name = 'Summary_PO_invoice_Batch_' + str(batch_id) + '_' + batch_id_str 
    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=3600)
    while datetime.datetime.now() <= check_time:
        try: 
            spt.change_file_name(Initial_path, str(summary_file_name) + ".csv",Target_path,'.csv')
            dl_sm_status = 'Successfully Download summary: Batch_' + str(batch_id) + '_' + batch_id_str 
            break
        except Exception:
            dl_sm_status = 'Failly Download summary: Batch_' + str(batch_id) + '_' + batch_id_str 
            pass
    print(dl_sm_status)
    #Cross Check Latest Invoice Summary
    # Get new summary
    os.chdir(r'D:\Download Management\Download PO Invoices Detail\Downloaded_Summary')
    os.getcwd()
    df_sm_new = pd.read_csv(summary_file_name + '.csv')
    #Convert to Float Actual Amount and Invocie Amount
    df_sm_new['Actual Paid Amount'] = df_sm_new['Actual Paid Amount'].str.replace('$','')
    df_sm_new['Actual Paid Amount'] = df_sm_new['Actual Paid Amount'].str.replace(",","")
    df_sm_new['Actual Paid Amount'] = df_sm_new['Actual Paid Amount'].str.replace(" ","")
    df_sm_new.loc[df_sm_new[df_sm_new['Actual Paid Amount'] == ''].index,'Actual Paid Amount'] = 0
    df_sm_new['Actual Paid Amount']  = pd.to_numeric(df_sm_new['Actual Paid Amount'] , downcast="float")
    
    df_sm_new['Invoice Amount'] = df_sm_new['Invoice Amount'].str.replace('$','')
    df_sm_new['Invoice Amount'] = df_sm_new['Invoice Amount'].str.replace(",","")
    df_sm_new['Invoice Amount'] = df_sm_new['Invoice Amount'].str.replace(" ","")
    df_sm_new.loc[df_sm_new[df_sm_new['Invoice Amount'] == ''].index,'Invoice Amount'] = 0
    df_sm_new['Invoice Amount']  = pd.to_numeric(df_sm_new['Invoice Amount'] , downcast="float")
    
    df_sm_new['Invoice Date'] = df_sm_new['Invoice Date'].astype('datetime64')
    df_sm_new['Due Date'] = df_sm_new['Due Date'].astype('datetime64')
    df_sm_new['Invoice Creation Date'] = df_sm_new['Invoice Creation Date'].astype('datetime64')
    
    
    df_sm_new = df_sm_new[['Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice #','Invoice Amount','Any Deductions']]
    
    df_sm_new.columns = ['Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']
    
    
    # Get Master summary
    df_sm_master_original = pd.read_excel(r'D:\Download Management\Download PO Invoices Detail\New Process\Database\3_sm_master.xlsx')
    df_sm_master = df_sm_master_original
    
    df_sm_master.loc[df_sm_master[df_sm_master['Actual Paid Amount'] == ' '].index,'Actual Paid Amount'] = 0
    df_sm_master['Actual Paid Amount']  = pd.to_numeric(df_sm_master['Actual Paid Amount'] , downcast="float")
    
    df_sm_master.loc[df_sm_master[df_sm_master['Invoice Amount'] == ' '].index,'Invoice Amount'] = 0
    df_sm_master['Invoice Amount']  = pd.to_numeric(df_sm_master['Invoice Amount'] , downcast="float")
    
    df_sm_master = df_sm_master[['Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']]
    
    
    
    
    # Check duplicated, new, update
    df_mer = df_sm_master.append(df_sm_new, ignore_index=True)
    
    list_dup_key = df_mer[df_mer['Invoice Number'].duplicated()]['Invoice Number']
    list_dup_full = df_mer[df_mer.duplicated()]['Invoice Number']
    list_update = list_dup_key[~list_dup_key.isin(list_dup_full)]
    list_new = df_sm_new[~df_sm_new['Invoice Number'].isin(list_dup_key)]['Invoice Number']
    
    
    # Update file new into full log:
    # Load current full log summary
    df_sm_fullog = pd.read_excel(r'D:\Download Management\Download PO Invoices Detail\New Process\Database\2_sm_fulllog.xlsx')
    
    df_sm_fullog.loc[df_sm_fullog[df_sm_fullog['Invoice Amount'] == ''].index,'Invoice Amount'] = 0
    df_sm_fullog['Invoice Date'] = df_sm_fullog['Invoice Date'].astype('datetime64')
    df_sm_fullog['Due Date'] = df_sm_fullog['Due Date'].astype('datetime64')
    df_sm_fullog['Invoice Creation Date'] = df_sm_fullog['Invoice Creation Date'].astype('datetime64')
    
    #Update status in new sum
    df_sm_new['Update_Status'] = np.nan
    df_sm_new['Batch_ID'] = batch_id
    df_sm_new['Withdraw_time'] = withdraw_time
    df_sm_new['ID'] = df_sm_new.index + df_sm_fullog['ID'].max() + 1
    
    df_sm_new.loc[df_sm_new[df_sm_new['Invoice Number'].isin(list_new)].index,'Update_Status'] = "New"
    df_sm_new.loc[df_sm_new[df_sm_new['Invoice Number'].isin(list_update)].index,'Update_Status'] = "Update"
    df_sm_new.loc[df_sm_new[df_sm_new['Invoice Number'].isin(list_dup_full)].index,'Update_Status'] = "Duplicated"
    df_sm_new = df_sm_new[['ID','Batch_ID','Withdraw_time','Update_Status', 'Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']]
    df_sm_new_tolog = df_sm_new
    df_sm_new_tolog['Detail_FlowID'] = np.nan # Đợi dò xong FlowID +  ghep additional info thì mới update vào log full
    
    df_sm_new_tolog = df_sm_new_tolog[['ID','Batch_ID','Withdraw_time','Update_Status','Detail_FlowID', 
                                       'Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                                       'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']]
    
    
    
    
    #Update master summary
    
    df_sm_new['Created_Datetime'] = df_sm_new.merge(df_sm_master_original, on='Invoice Number', how='left')['Created_Datetime']
    df_sm_new['Detail_Download_Status'] = 'Not Yet'
    df_sm_new['Detail_FlowID'] = np.nan
    
    df_sm_new = df_sm_new[['ID','Batch_ID','Created_Datetime','Withdraw_time','Update_Status', 'Detail_Download_Status','Detail_FlowID',
                           'Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']]
    df_sm_new .columns = ['ID','Batch_ID','Created_Datetime','Last_Modified_Datetime','Update_Status', 'Detail_Download_Status','Detail_FlowID',
                           'Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                             'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions']
    df_sm_new.loc[df_sm_new[df_sm_new['Created_Datetime'].isnull()].index,'Created_Datetime'] = withdraw_time
    df_sm_new_to_master = df_sm_new[df_sm_new['Update_Status'] != 'Duplicated']
    df_sm_master_original = df_sm_master_original.drop(df_sm_master_original[df_sm_master_original['Invoice Number'].isin(list_update)].index)
    df_sm_master_original =  df_sm_master_original.append(df_sm_new_to_master, ignore_index=True)
    
    
    
    print("\n----- SUMMARY UPDATE LOG STATUS -----")
    print(df_sm_new.groupby(['Update_Status'])['Update_Status'].count())
    
    
    
    
    
    
    ####################################################################################################
    #DOWNLOAD INVOICE DETAIL FILES
    print("\n----- DOWNLOAD INVOICE DETAILS -----")
    # Start Download invoice detail
    print(df_sm_master_original.groupby(['Detail_Download_Status'])['Detail_Download_Status'].count())
    
    df_PoInv = df_sm_master_original[df_sm_master_original['Detail_Download_Status']=='Not Yet'][['Invoice Number','Detail_Download_Status']]
    df_PoInv.columns = ['Invoice #','Download Status']
    
    
    
    print('\n======================\n')
    print("Total: " + str(len(df_PoInv)) + ' Invoices')
    print(df_PoInv.groupby(['Download Status']).count())
    
    print('\n===========START DOWNLOAD DETAIL FILES===========\n')
    # vào từng invoice 1 để tải
    driver.switch_to.window(driver.window_handles[0])
    driver.implicitly_wait(10)
    driver.find_element_by_xpath('//*[@id="search-criteria"]/span/span').click()
    
    for search_criteria in range(5):
        driver.implicitly_wait(10)
        search_criteria_option = driver.find_element_by_xpath('//*[@id="search-criteria_' + str(search_criteria) + '"]')
        if search_criteria_option.text == 'Invoice Number':
            search_criteria_option.click()
            break
    driver.implicitly_wait(100)
    
    #Initial_path =R"D:\Download Management\Temporary_Nguyen Y"
    
    Target_path = R'D:\Download Management\Download PO Invoices Detail\Downloaded Files\Batch_' + str(batch_id)
    try:
        os.mkdir(Target_path)
    except:
        pass
    
    # set up biến ngoại: đếm cho processing
    count_full = 0
    count = 0
    processing_list = ''
    batch_no = 20
    add_summary_title = ['Terms', 'Qty variance amount (shortage claim)',
                        'Price variance amount (price claim)','Input variance amount',
                        'Approved date','Arrival date']
    add_summary_detail = []
    df_sm_new_tolog = df_sm_new_tolog.reindex(columns = df_sm_new_tolog.columns.tolist() + add_summary_title)
    
    df_sm_new_tolog = df_sm_new_tolog[['ID','Batch_ID','Withdraw_time','Update_Status','Detail_FlowID', 
                                       'Marketplace','Invoice Date','Due Date','Invoice Status','Source','Actual Paid Amount',
                                       'Payee','Invoice Creation Date','Invoice Number','Invoice Amount','Any Deductions',
                                       'Terms', 'Qty variance amount (shortage claim)',
                                        'Price variance amount (price claim)','Input variance amount',
                                        'Approved date','Arrival date']]
    
    
    #Search invoice number to download as list provided
    for invoice_ID in df_PoInv['Invoice #']:
        count_full = count_full +1
        # Check Done Download
        if df_PoInv[df_PoInv['Invoice #'] == invoice_ID].iloc[0]['Download Status'] == 'Not Yet':
            # Search invocie id
            driver.switch_to.window(driver.window_handles[0])
            
            #Get option
            try_action = 0   
            while try_action <50:
                try_action = try_action + 1
                if try_action >1:
                        print('Try search again: ' + str(try_action))            
                try:
                    driver.implicitly_wait(5)
                    search = driver.find_element_by_xpath('//*[@id="invoice-number"]')
                    search.clear()
                    search.send_keys(invoice_ID)
                    driver.implicitly_wait(5)
                    button_search = driver.find_element_by_xpath('//*[@id="advancedSearchHarmonicForm-submit"]')
                    button_search.click() # click search
                    # Click action
                    driver.implicitly_wait(10)
                    action = driver.find_element_by_xpath('//*[@id="a-autoid-0"]/span/input')
                    try_action = 50
                except TimeoutException:
                    print("Refresh finding due to fail")
                    driver.refresh()
                except NoSuchElementException:
                    print("Refresh finding due to fail")
                    driver.refresh()
            
            # Open option link
            try_action = 0        
            while try_action < 50:   
                try_action = try_action + 1
                if try_action >1:
                    print('Try get option again: ' + str(try_action))
                    sleep(2)
                try: 
                    #print(str(invoice_ID) + ' : Click action ||| ' + str(datetime.datetime.now()))
                    action.click()
                    #driver.implicitly_wait(100)
                    # Tìm invoice detail
                    for ele_no in range(4):
                        #(str(invoice_ID) + ': Start finding Ele no: ' + str(ele_no + 1) + '||| ' + str(datetime.datetime.now()))
                        ele_name = '//*[@id="a-popover-content-1"]/div/a[' + str(ele_no + 1) + ']'
                        driver.implicitly_wait(5)
                        option = driver.find_element_by_xpath(ele_name)
                        #//*[@id="a-popover-content-1"]/div[1]/div/div
                        #print(str(invoice_ID) + ' : Done finding with text ' + option.text +' ||| ' + str(datetime.datetime.now()))
                        if option.text == "View invoice details":
                            #print(option.text + " " + str(index))
                            option.click()
                            driver.implicitly_wait(10)
                            try_action = 50
                            #print(str(invoice_ID) + ' : Done click with text: ' + option.text + ' ||| ' + str(datetime.datetime.now()))
                            count = count + 1
                            break
                    if option.text != "View invoice details":
                        print('Dont Found detail - '+ option.text + ' ||| Try: ' +str(try_action) + 'times')
                        driver.find_element_by_xpath('//*[@id="a-popover-1"]/div/header/button').click
                        
                except TimeoutException:
                    driver.implicitly_wait(5)
                    driver.find_element_by_xpath('//*[@id="a-popover-1"]/div/header/button').click()
                    print("Refresh option due to fail")
                except NoSuchElementException:
                    driver.implicitly_wait(5)
                    driver.find_element_by_xpath('//*[@id="a-popover-1"]/div/header/button').click()
                    print("Refresh option due to fail")
    
    
            # Tạo vòng lặp, cứ 20 file tải 1 
            if count == batch_no or count_full == (len(df_PoInv)):
                sleep(5)
                # Tải từng file trong tab chrome detail
                while count >= 1:
                    driver.switch_to.window(driver.window_handles[1])
                    url_link = ''
                    start_position = 0
                    end_position = 0
                    try_count_download = 5
                    while try_count_download >= 1:
                        try:
                            #Click Download
                            driver.implicitly_wait(5)
                            invoice_num = driver.find_element_by_xpath('//*[@id="sc-content-container"]/div/div[1]/div/h1').text
                            driver.implicitly_wait(5)
                            driver.find_element_by_xpath('//*[@id="line-items-export-to-spreadsheet-announce"]').click()
                            try_count_download = 0
                            
                            # Get additional information for summary
                            driver.implicitly_wait(3)
                            table_ele = driver.find_elements_by_xpath('//*[@class="a-color-base invoice-property-field" or @class="a-column a-span6"]')
                            add_summary_detail = []
                            
                            for title_ori in range(len(add_summary_title)):
                                add_summary_detail.append(np.nan)
                                
                            for title_ori in range(len(add_summary_title)):
                                for title_web in range(len(table_ele)):
                                    if add_summary_title[title_ori] == table_ele[title_web].text:
                                        add_summary_detail[title_ori] = table_ele[title_web + 1].text
                            
                            
                            
                            # Get FlowID
                            url_link = driver.current_url                        
                            start_position  = url_link.find('invoice-details?workflowStateId=') + len('invoice-details?workflowStateId=')
                            end_position = url_link.find('&activeTab=')
                            detail_flowid = url_link[start_position:end_position]
                            df_sm_new_tolog.loc[df_sm_new_tolog[df_sm_new_tolog['Invoice Number'] == invoice_num].index,'Detail_FlowID'] = detail_flowid
                            df_sm_new_tolog.loc[df_sm_new_tolog[df_sm_new_tolog['Invoice Number'] == invoice_num].index,add_summary_title] = add_summary_detail
                            
                        except TimeoutException:
                            #refresh page:
                            print("Refresh page detail due to fail")
                            driver.refresh()
                            try_count_download = try_count_download - 1
                            sleep(5)
                        except NoSuchElementException:
                            #refresh page:
                            print("Refresh page detail due to fail")
                            driver.refresh()
                            try_count_download = try_count_download - 1
                            sleep(5)
                    # Đợi 600 giây đến khai tải xong rồi đổi tên
                    check_time = datetime.datetime.now()  + datetime.timedelta(seconds=600)
                    while datetime.datetime.now() <= check_time:
                        try: 
                            spt.change_file_name(Initial_path, str(invoice_num) + ".csv",Target_path,'.csv')
                            download_status = 'Downloaded'
                            break
                        except Exception:
                            download_status = 'Failed'
                            pass
                    df_PoInv.loc[df_PoInv[df_PoInv['Invoice #'] == invoice_num].index,'Download Status'] = download_status
                    df_sm_master_original.loc[df_sm_master_original[df_sm_master_original['Invoice Number'] == invoice_num].index,'Detail_Download_Status'] = download_status
                    df_sm_master_original.loc[df_sm_master_original[df_sm_master_original['Invoice Number'] == invoice_num].index,'Detail_FlowID'] = detail_flowid
                    df_sm_master_original.loc[df_sm_master_original[df_sm_master_original['Invoice Number'] == invoice_num].index,add_summary_title] = add_summary_detail
                    
                    
                    print("Done Files: " + str(count_full - count + 1) + '/' + str(len(df_PoInv)) + ' ||| #: ' + str(invoice_num) + ' - Batch ' + str(batch_id))
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    count = count - 1
        
        
    
    
    print('\n==========SUMMARY DOWNLOAD============')
    #print(df_PoInv.groupby(['Download Status']).count())
    print(df_sm_master_original.groupby(['Detail_Download_Status'])['Detail_Download_Status'].count())
    print('\n==========DONE DOWNLOAD============')
    
    
    
    # Update Log Full
    df_sm_fullog = df_sm_fullog.append(df_sm_new_tolog, ignore_index=True)
    
    # Format data master
    spt.convert_text_to_number(df_sm_master_original,'Qty variance amount (shortage claim)','float')
    spt.convert_text_to_number(df_sm_master_original,'Price variance amount (price claim)','float')
    spt.convert_text_to_number(df_sm_master_original,'Input variance amount','float')
    try: 
        df_sm_master_original['Approved date'] = df_sm_master_original['Approved date'] .str.replace('-','')
        df_sm_master_original['Approved date'] = df_sm_master_original['Approved date'].astype('datetime64[ns]')
    except:
        pass
    try:
        df_sm_master_original['Arrival date'] = df_sm_master_original['Arrival date'] .str.replace('-','')
        df_sm_master_original['Arrival date'] = df_sm_master_original['Arrival date'].astype('datetime64[ns]')
    except:
        pass
    #Format data full log:
    spt.convert_text_to_number(df_sm_fullog,'Qty variance amount (shortage claim)','float')
    spt.convert_text_to_number(df_sm_fullog,'Price variance amount (price claim)','float')
    spt.convert_text_to_number(df_sm_fullog,'Input variance amount','float')
    
    try:
        df_sm_fullog['Approved date'] = df_sm_fullog['Approved date'] .str.replace('-','')
        df_sm_fullog['Approved date'] = df_sm_fullog['Approved date'].astype('datetime64[ns]')
    except:
        pass
    
    try:
        df_sm_fullog['Arrival date'] = df_sm_fullog['Arrival date'] .str.replace('-','')
        df_sm_fullog['Arrival date'] = df_sm_fullog['Arrival date'].astype('datetime64[ns]')
    except:
        pass
    
    #write df_sm_master & Full log into db
    os.chdir(r'D:\Download Management\Download PO Invoices Detail\New Process\Database')
    spt.write_excel(df_sm_master_original,'3_sm_master')
    spt.write_excel(df_sm_fullog,'2_sm_fulllog')
    spt.write_excel(df_sm_batch_man,'1_dl_sm_batch_man')
    
    return batch_id
    # COMBINE ALL DETAIL FILES
    
    
    
def combine_detail_files(batch_id, template_url, detail_folder_path, target_output_url):
    #config batch
    #batch_id = 22
    batch_folder = 'Batch_' + str(batch_id)
    # config: folder of list 
    # Config file template dau vao
    
    # Config file outcome dau ra: duong dan folder
    
    
    import pandas as pd
    import os
    import csv
    import xlrd
    import numpy as np
    
    
    print('\n__________START COMBINE ' + str(batch_folder) + '___________\n')
    # get the list file
    list_filename = [f for f in os.listdir(detail_folder_path) if f.endswith('.csv')]
    
    mer_ProMan = pd.DataFrame(list_filename , columns =['File name'])  
    mer_ProMan['Invoice #'] = mer_ProMan['File name'].str.replace('.csv','')
    
    # create template df
    df_mer = pd.read_excel(template_url)
    
    # xác định nguồn folder của detail file
    os.chdir(detail_folder_path)
    os.getcwd()
    
    arrCol = ['PO #','External ID','Title','ASIN','Model #','Freight Term','Qty','Unit Cost','Amount','Shortage quantity','Amount shortage','Last received date','ASIN received','Quantity received','Unit cost','Amount received']
    
    # collect detail file 
    i = 1
    for invoice_num in mer_ProMan['Invoice #']:
        try:
            file = open(invoice_num + '.csv', encoding='Latin1')            
            
            reader = csv.reader(file)
            
            for p in range(3):
                next(reader)
                
            csvFile = list(reader)
            csvFile_T = csvFile
            file.close()
            
            flagNE = False
            flagCase = 0
            numRow = 0
            maxLen = 0
            for row in csvFile:
                if len(row) < 16:
                    flagCase = 1
                    flagNE = True
                
                if len(row) == 16:
                    flagCase = 2
                
                if len(row) == 17:
                    if csvFile[numRow][16].strip() == '':
                        flagCase = 2
                    else:
                        flagCase = 3
                
                if len(row) > 17:
                    flagCase = 3
                    
                ##########################################################
                newRow = []
                if flagCase == 1:
                    if csvFile[numRow][6] == 'Collect' or csvFile[numRow][6] == 'Prepaid':
                        updRow = []
                        numb = 2
                        strTitle = ''
                        strAsin = ''
                        
                        for k in range(len(row)):
                            if k >= 2 and k < 2 + numb:
                                if strTitle != '':
                                    strTitle += ", " + csvFile[numRow][k]
                                else:
                                    strTitle = csvFile[numRow][k]
                            else:
                                if k == 2 + numb:
                                    updRow.append(strTitle)
                                    updRow.append(csvFile[numRow][k])
                                else:
                                    updRow.append(csvFile[numRow][k])
                                            
                        newRow = updRow
                        
                    if maxLen < len(row):
                        maxLen = len(row)
                
                if flagCase == 2:
                    newRow = row[:16]
                
                if flagCase == 3:
                    numb = len(row) - 16
                    
                    if csvFile[numRow][len(row)-1].strip() == '':
                        numb = numb - 1
                
                    strTitle = ''
                    for k in range(len(row)):
                        if k >= 2 and k <= 2 + numb:
                            if strTitle != '':
                                strTitle += ", " + csvFile[numRow][k]
                            else:
                                strTitle = csvFile[numRow][k]
                        else:
                            if k == 3 + numb:
                                newRow.append(strTitle)
                                newRow.append(csvFile[numRow][k])
                            else:
                                if k == len(row)-1 and csvFile[numRow][k].strip() != '':
                                    newRow.append(csvFile[numRow][k])
                                else:
                                    if k < len(row)-1:
                                        newRow.append(csvFile[numRow][k])
                
                if newRow != '':
                    csvFile[numRow] = newRow
                
                numRow += 1
                
            
            newList = [v for v in csvFile if len(v) > 15]
            
            if flagCase >= 2:
                maxLen = 16
            
            df_newList = pd.DataFrame(newList, columns=arrCol[:maxLen])
            df_mer = df_mer.append(df_newList, ignore_index=True)
            
            df_mer.loc[df_mer[df_mer['Invoice #'].isnull()].index,'Invoice #'] = invoice_num
        except Exception as e:
            print('ERROR: ' + invoice_num)
            print(e)
            print(invoice_num)
            break
        print(str(i).zfill(len(str(len(mer_ProMan)))) + '  /  ' + str(len(mer_ProMan)))
        i = i + 1
            
    # get ID, batch ID for mer batch
    df_mer['ID'] = df_mer.index + 1
    df_mer['Batch_ID'] = batch_id
    

    # Convert text into number
    spt.convert_text_to_number(df_mer,'Qty','int')
    spt.convert_text_to_number(df_mer,'Unit Cost','float')
    spt.convert_text_to_number(df_mer,'Amount','float')
    spt.convert_text_to_number(df_mer,'Shortage quantity','int')
    spt.convert_text_to_number(df_mer,'Amount shortage','float')
    df_mer['Last received date'] = df_mer['Last received date'].astype('datetime64[ns]')
    spt.convert_text_to_number(df_mer,'Quantity received','int')
    spt.convert_text_to_number(df_mer,'Unit cost','float')
    spt.convert_text_to_number(df_mer,'Amount received','float')
    
    
    # LOAD DATA TO DATABASE
    df_mer.to_excel(target_output_url, sheet_name='Data', index=False)
    print('\n_____________COMPLETED ' + batch_folder + '______________\n')

def update_detail_into_db(batch_id, url_new_batch, url_detail_fullog, url_detail_master):
    
    # CONFIG INPUT
    #path_db = r'D:\Download Management\Download PO Invoices Detail\New Process\Database'
    #batch_id = batch_id + 1
    #batch_id = 22
    batch_file = 'Batch_' + str(batch_id)
    #url_new_batch = r'G:\My Drive\BII Team\Business Intelligence Investment\Downloaded\AMZ\9_PO_Invoice\New Process\Download\Output_combine' +'\\' + batch_file + '.xlsx'
    
    #url_detail_fullog = r'D:\Download Management\Download PO Invoices Detail\New Process\Database\4_detail_full_log.xlsx'
    
    #url_detail_master = r'D:\Download Management\Download PO Invoices Detail\New Process\Database\5_detail_master.xlsx'
    
    
    
    
    print('\n_____________START UPDATE ' + batch_file + '______________\n')
    
    import pandas as pd
    import os
    import numpy as np
    
    
    df_detail_fullog = pd.read_excel(url_detail_fullog)
    
    df_detail_master = pd.read_excel(url_detail_master)
    
    df_new_batch = pd.read_excel(url_new_batch)
    
    print('Original master before: ' +str(len(df_detail_master)))
          
    # ADJUST ID CONTINUOUSLY WITH FULL LOG
    df_new_batch['ID'] = df_new_batch['ID'] + df_detail_fullog['ID'].max()
    
    # ADD NEW BATCH INTO FULL LOG
    df_new_batch = df_new_batch[['ID', 'Batch_ID', 'Invoice #', 'PO #', 
                                 'External ID', 'Title', 'ASIN', 'Model #', 'Freight Term', 'Qty', 'Unit Cost', 'Amount', 
                                 'Shortage quantity', 'Amount shortage', 'Last received date', 'ASIN received', 
                                 'Quantity received', 'Unit cost', 'Amount received']]
    
    df_detail_fullog = df_detail_fullog.append(df_new_batch, ignore_index=True)
    
    # UPDATE NEW BATCH INTO MASTER
    # Drop duplicate Invoice #
    update_list = df_detail_master[df_detail_master['Invoice #'].isin(df_new_batch['Invoice #'])]['Invoice #'].unique()
    
    df_detail_master = df_detail_master.drop(df_detail_master[df_detail_master['Invoice #'].isin(update_list)].index)
    
    # Update New batch into master
    df_detail_master = df_detail_master.append(df_new_batch, ignore_index=True)
    
    
    # LOAD DATA TO DATABASE
    df_detail_fullog.to_excel(url_detail_fullog, sheet_name='detail_full_log', index=False)
    df_detail_master.to_excel(url_detail_master, sheet_name='detail_master', index=False)
    
    print('New Batch: ' + str(len(df_new_batch)))
    print('Update: ' + str(len(update_list)))
    print('Original master after: ' +str(len(df_detail_master)))
          
    
    print('\n_____________COMPLETED UPDATE ' + batch_file + '______________\n')
    
    
