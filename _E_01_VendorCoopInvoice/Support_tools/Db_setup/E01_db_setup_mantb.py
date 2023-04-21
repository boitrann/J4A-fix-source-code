# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 10:32:08 2021

@author: Guest_1
"""



import cx_Oracle
import pandas as pd
import os
import numpy as np
import datetime
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
from support_tool.Connection import df_to_oratb as dto_spt


def E01_NO_RECORD_DETAIL():
    from support_tool.Connection import query as query
    from support_tool.Connection import Connect_y4abii as biicnn   
    #tb = db_info['no_rec']['tb']
    full = 'E01_NO_RECORD_DETAIL'
    
    sql = """CREATE TABLE """ + full + """ (
                        INVOICE_NUMBER varchar2(255), 
                        LAST_REQ_ID number(30),
                        LAST_WITHDRAW_TIME timestamp,
                        COUNT_NO_REC number(30),
                        PRIMARY KEY(INVOICE_NUMBER)) """
    try:
        query.run_sql(biicnn, """drop table """ + full)
    except:
        pass
    query.run_sql(biicnn, sql)       
# E01_NO_RECORD_DETAIL()
def create_table_reqman_collect_overall():
    db_info = o_spt.db_info()
    ora_table = db_info['OVR']['req']
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        REQ_ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REQ_TIME timestamp,
                        STATUS varchar2(255),
                        FROM_DATE timestamp,
                        TO_DATE timestamp,
                        START_TIME timestamp,
                        FINISH_TIME timestamp,
                        COLLECTED_FILE_NAME varchar2(2000),
                        CONVERTED_FILE_NAME varchar2(2000),
                        REMARKS varchar2(1000),
                        PRIMARY KEY(REQ_ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    
    connection = None         

def create_table_req_detail():
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['req']
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        REQ_ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REQ_TIME timestamp,
                        REQ_SOURCE varchar2(1000),
                        STATUS varchar2(255),
                        ASSIGNMENT_STATUS varchar2(255),
                        START_TIME timestamp,
                        FINISH_TIME timestamp,
                        REMARKS varchar2(1000),
                        PRIMARY KEY(REQ_ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    
    connection = None       
    
    
def create_table_req_detail_list():
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['req_detail']
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REQ_ID NUMBER(30) ,
                        REQ_SOURCE varchar2(1000),
                        INVOICE_NUMBER varchar2(255),
                        STATUS_SHORTAGE varchar2(255),
                        STATUS_DETAIL varchar2(255),
                        ASSIGNED_BATCH NUMBER(30),
                        ASSIGNED_BOT varchar2(255),
                        WITHDRAW_TIME timestamp,
                        REMARKS varchar2(1000),
                        PRIMARY KEY(ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    
    connection = None     
    
def create_table_collecteddetail_files():
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['detailfile_man']
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        REQ_ID NUMBER(30),
                        INVOICE_NUMBER varchar2(255),
                        FILE_NAME varchar2(1000),
                        WITHDRAW_TIME timestamp,
                        TYPE varchar2(1000),
                        NOT_DEF_CHECK NUMBER(1),
                        REMARKS varchar2(1000),
                        PRIMARY KEY(ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    connection = None     
    


def create_table_transittb_man():
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['trantb_man']
    print(ora_table)
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        TYPE_CODE varchar2(255),
                        DESCRIPTIONS varchar2(2000),
                        COST_TYPE varchar2(255),
                        STORAGE_REQUIREMENT NUMBER(1),
                        TRANSIT_REQUIREMENT NUMBER(1),
                        NOT_NULL_COL varchar2(30),
                        TRANS_TABLE varchar2(255),
                        DEST_TABLE varchar2(255),
                        REMARKS varchar2(1000),
                        PRIMARY KEY(ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    connection = None 


def create_table_transitcols_man():
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['trancol_man']
    print(ora_table)
    from support_tool.Connection import Connect_y4abii as connection
    sql = """CREATE TABLE """ + ora_table + """ (
                        ID NUMBER(30) GENERATED ALWAYS AS IDENTITY, 
                        TYPE_CODE varchar2(255),
                        TRANS_COL varchar2(30),
                        DEST_COL varchar2(30),
                        REMARKS varchar2(1000),
                        PRIMARY KEY(ID)) """
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    connection = None 



def inport_ovr_req():
    excel_file = '1_avccoopinv_batch_man'
    excel_path = r'D:\Download Management\Avc_CoopInvoices\Database'

    from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
    
    from support_tool.Connection import df_to_oratb as dto_spt
    db_info = o_spt.db_info()
    ora_table = db_info['OVR']['req']
    
    
    os.chdir(excel_path)
    df_input = pd.read_csv(excel_file + '.csv',encoding='cp1252')
    
    dto_spt.df_to_oratable(df_input, ['REQ_ID'], ora_table, True)
    
    


def import_collected_detfile_man():
    
    excel_file = '99_list_file_invoice_detail_db'
    excel_path = r'D:\Download Management\Avc_CoopInvoices\Database'
    exclusive_cols = ['ID']
    
    from support_tool.Connection import df_to_oratb as dto_spt
    
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['detailfile_man']
    
    
    os.chdir(excel_path)
    df_input = pd.read_excel(excel_file + '.xlsx')
    
    dto_spt.df_to_oratable(df_input, exclusive_cols, ora_table, True)

    
def import_tran_tb_man():    
    
    excel_file = '99_Transitman_tatbles'
    excel_path = r'D:\Download Management\Avc_CoopInvoices\Database'
    exclusive_cols = ['ID']
    
    from support_tool.Connection import df_to_oratb as dto_spt
    
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['trantb_man']
    
    
    os.chdir(excel_path)
    df_input = pd.read_excel(excel_file + '.xlsx')
    
    dto_spt.df_to_oratable(df_input, exclusive_cols, ora_table, True)
        
        
def import_tran_col_man():          
    excel_file = '99_Transitman_cols'
    excel_path = r'D:\Download Management\Avc_CoopInvoices\Database'
    exclusive_cols = ['ID']
    
    db_info = o_spt.db_info()
    ora_table = db_info['DET']['trancol_man']
    
    os.chdir(excel_path)
    df_input = pd.read_excel(excel_file + '.xlsx')
    
    dto_spt.df_to_oratable(df_input, exclusive_cols, ora_table, True)
    
        
    
    