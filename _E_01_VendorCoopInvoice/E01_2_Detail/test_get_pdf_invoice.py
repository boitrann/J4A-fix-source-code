# -*- coding: utf-8 -*-
"""
# @ Author: Huu Khue                                                      
# @ Created Date:   2/7/2022 2:14 PM                                                      
# @ Description: diễn giải về tính năng của function       
# @ Return: nêu rõ kết quả trả về của function sẽ là gì  
"""
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
from login_page.login_amz_vendor_central import login_with_cookies as ls
from time import  sleep
from support_tool import support_tool as spt

fd_cf = o_spt.folder_config()
temp_dl_path=r'D:\Download_temp'
pdf_path = fd_cf['pdf_path']
url =r'https://vendorcentral.amazon.com/hz/vendor/members/coop?searchText=6267-526547590%0D%0A6267-559690610%0D%0A6267-559691955&from_date_m=12&from_date_d=1&from_date_y=2021&to_date_m=1&to_date_d=19&to_date_y=2022'
driver = ls(temp_dl_path)
driver.get(url)
tables = driver.find_elements_by_xpath('//*[@id="cc-invoice-table"]/div[5]/div/table/tbody/tr')
for table in tables[1:]:
    invoice_num = table.find_element_by_xpath('td[2]').text
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
        driver.implicitly_wait(0.5)
        table.find_elements_by_xpath('/td[7]/div/span').click()
        # print(download_shortage_status)

        sleep(0.5)
        driver.implicitly_wait(1)
        invoice_pdf = driver.find_element_by_id('invoiceDownloads-' + invoice_num + '_0')
    if (invoice_pdf.text == 'Invoice as a PDF'):
        invoice_pdf.click()
    sleep(3)
    spt.change_file_name(temp_dl_path, str(invoice_num) + ".pdf", pdf_path,
                         '.pdf')