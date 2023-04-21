# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 08:51:20 2021

@author: Guest_1
"""
import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')
from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn    
from support_tool.Connection import Connect_be3 as be3cnn    

def connection():
    import MySQLdb
    connection = MySQLdb.connect(host="backend3.yes4all.com",      # your host, usually localhost
                     user="user",                 # your username
                     passwd="pass",
                     charset='utf8'# your password
                     )   
    
    
    cursor = connection.cursor()
    
    return connection, cursor

# connection()

def update_asin_table():
    df_asinbe = query.query(be3cnn, """select now() as UPDATE_TIME, a.asin as ASIN,p.id as PRODUCT_ID, p.title as PRODUCT_TITLE, p.product_sku as PRODUCT_SKU
                    from yes4all_besystem.asin a join yes4all_besystem.products p on a.product_id = p.id""")
                    
    df_current_asin =  query.query(biicnn, """select * from SYSTEM.Z_ASIN_SKU""")
    
    df_asinbe = df_asinbe.drop(df_asinbe[df_asinbe['ASIN'].isin(df_current_asin['ASIN'])].index)
    
    if len(df_asinbe)>0:
        query.df_to_db(biicnn, df_asinbe, [], 'SYSTEM','Z_ASIN_SKU', False)


def db_type():
    return 'mysql'