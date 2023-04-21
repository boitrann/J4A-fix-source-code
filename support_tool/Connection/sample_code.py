# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 07:17:45 2021

@author: Guest_1
"""
import pandas as pd
from support_tool.Connection import Connect_y4abii as connection
from support_tool.Connection import query as query

sql ="""
 select ORDER_ID ,INVOICE_ID ,ASIN ,PAYMENT_TERM,TOTAL_AMOUNT, max(REQ_ID) ID
     from SYSTEM.C02_DSINVOICE_FULLOG_WEB
     where PAYMENT_TERM  is not null and INVOICE_ID in (
      
      select INVOICE_ID  from system.C02_DSINVOICE_MASTER_FIN where PAYMENT_TERM is NULL 
      )
      group by ORDER_ID ,INVOICE_ID ,ASIN,payment_term,TOTAL_AMOUNT
"""

df = query.query(connection,sql)
print(df)
lst_req_id= list(df['ID'].unique())
print(lst_req_id)
# for id in lst_req_id:
#     sql =f"""
#     MERGE INTO SYSTEM.c02_dsinvoice_master_fin  t1
#         USING (
#         select invoice_id, asin,
#                 PAYMENT_TERM, PAYEE_CODE, TOTAL_AMOUNT
#                from SYSTEM.c02_dsinvoice_fullog_web
#                where req_id = {id} and payment_term is not null) t2
#         on (t1.invoice_id = t2.invoice_id and t1.asin = t2.asin)
#         when matched  then
#             update  set
#                         t1.SOURCE = 'WEB_COLLECT',
#                         t1.PAYMENT_TERM = t2.PAYMENT_TERM,
#                         t1.PAYEE_CODE = t2.PAYEE_CODE,
#                         t1.TOTAL_AMOUNT = t2.TOTAL_AMOUNT
#                        ;
#
#     """