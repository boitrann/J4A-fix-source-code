# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 23:18:42 2021

@author: Guest_1
"""

from support_tool.Connection import Connect_y4abii as connection
import pandas as pd
import datetime


# CURRENTLY SUPPORT 4 TYPE OF DATA: TEXT, DATETIME, INT, FLOAT

"""
# CONFIG NOT IMPORT COLUMNS (EG: AUTO ID)
exclusive_cols = []
ora_table = 'TEST_IMPORT_DF'
df_new = pd.DataFrame([], columns = [])

###
import os
excel_path = r'D:\Download Management\_A02_Retail Sales Invoices\Database'
excxcel_file = '2_sm_fulllog'
os.chdir(excel_path)
df_new = pd.read_excel(excxcel_file + '.xlsx')

ora_table = 'TEST_IMPORT_DF'
exclusive_cols = ['WITHDRAW_TIME']
###
"""
def df_to_oratable(df_new, exclusive_cols, ora_table, announce):
    # select data type from ora table
    sql_select = "SELECT  table_name, column_name, data_type FROM all_tab_columns where table_name = '" + ora_table + "'"
    cnn, cursor = connection.connection()
    df_cols = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None
    
    # define 4 LIST: text, datime, int, float
    text_type_def = ['VARCHAR2', 'NVARCHAR2']
    datetime_type_def = ['TIMESTAMP(6)']
    int_type_def = ['NUMBER']
    float_type_def = ['FLOAT']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    data_input_cols = df_new.columns
    data_output_cols = []
    
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name not in exclusive_cols and col_name in data_input_cols:
            data_output_cols = data_output_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    # DEFINE DF DATA FROM DF NEW WITH THESE COLUMNS OF 4 TYPES
    df_data = pd.DataFrame([], columns = data_output_cols)
    df_data = df_data.append(df_new[data_output_cols], ignore_index = True)
    
    # CONVERT DATA BEFORE IMPORT
    # Convert Datetime cols
    df_data[date_time_cols] = df_data[date_time_cols].astype('datetime64[ns]')
    
    for col in date_time_cols:
        df_data[col] = df_data[col].dt.strftime('%d-%m-%Y %H:%M:%S')
    df_data[date_time_cols] = df_data[date_time_cols].astype(str)
    df_data[date_time_cols] = df_data[date_time_cols].replace({'nan': None})
    
    
    # Convert Text cols
    #text_cols = [ 'TYPE_1','TYPE_2', 'STATUS', 'COLLECTED_FILE_NAME', 'CONVERTED_FILE_NAME',  'REMARKS']
    df_data[text_cols] = df_data[text_cols].astype(str)
    df_data[text_cols] = df_data[text_cols].replace({'nan': None})
    df_data[text_cols] = df_data[text_cols].replace({'None': None})
    
    # Convert Int cols
    for col in int_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='signed')
    df_data[int_cols] = df_data[int_cols].fillna(0)
    
    # Convert Float cols
    for col in float_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='float')
    df_data[float_cols] = df_data[float_cols].fillna(0)
    
    
    df_data = df_data.where(pd.notnull(df_data), None)
    
    
    
    # MAKE ORACLE SQL END
    ora_cols = """ID, REQ_ID, WITHDRAW_TIME, UPDATE_STATUS, LAST_ID, DETAIL_FLOWID, MARKETPLACE, INVOICE_DATE, 
                DUE_DATE, INVOICE_STATUS, SOURCE, ACTUAL_PAID_AMOUNT, PAYEE, INVOICE_CREATION_DATE, INVOICE_NUMBER, INVOICE_AMOUNT, 
                ANY_DEDUCTIONS, TERMS, QTY_VAR_AMOUNT_SHORTAGE_CLAIM, PRICE_VAR_AMOUNT_PRICE_CLAIM, INPUT_VAR_AMOUNT, APPROVED_DATE, ARRIVAL_DATE"""
    ora_values = """:1, :2, TO_DATE(:3,'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, TO_DATE(:8,'YYYY-MM-DD HH24:MI:SS'),
                     TO_DATE(:9,'YYYY-MM-DD HH24:MI:SS'), :10, :11, :12, :13, TO_DATE(:14,'YYYY-MM-DD HH24:MI:SS'), :15, :16, :17, :18, :19, :20, :21, 
                     TO_DATE(:22,'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:23,'YYYY-MM-DD HH24:MI:SS')"""
    
    sql_end = """ (""" + ora_cols + """) values (""" + ora_values + """)"""
    
    ora_cols_text = ','.join(data_output_cols)
    count = 0
    ora_values_list = []
    for col in data_output_cols:
        count = count +1    
        if col in   date_time_cols:
            value = "TO_DATE(:" + str(count) + ",'YYYY-MM-DD HH24:MI:SS')"
        else:
            value = ":" + str(count)
        ora_values_list = ora_values_list + [value]
    ora_values_text = ','.join(ora_values_list)
    sql_end = """ (""" + ora_cols_text + """) values (""" + ora_values_text + """)"""
    
    # RUN SQL INPORT
    df_data = df_data[data_output_cols]
    list_data = df_data.values.tolist()
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {ora_table}')
    sql_insert = 'insert into ' + ora_table + sql_end
    cnn, cursor = connection.connection()
    cursor.executemany(sql_insert, list_data)
    cnn.commit()
    cnn = None
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')
    list_data = None
    sql_insert = None
