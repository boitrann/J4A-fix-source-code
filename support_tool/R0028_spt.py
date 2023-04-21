# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 06:42:46 2021

@author: NGUYEN Y
"""



import datetime


from support_tool.Connection import Connect_be3 as be3cnn
from support_tool.Connection import Connect_y4abii as biicnn
# from support_tool.Connection import Connect_y4abii_SCsc as biicnnsc
from support_tool.Connection import Connect_y4abii_SC as clcnn
from support_tool.Connection import query as query

def sync_table(table,task_id):
    print(table)
    tracking_time = {}
    tracking_time['TABLE_NAME'] = table
    # Get table
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Get table')
    df = query.query(biicnn, """select * from SYSTEM.""" + table)
    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    tracking_time['DURATION_GET'] = int(second_process)
    tracking_time['LEN_INSERT'] = len(df)

    # Truncate table
    len_truncate = query.query(clcnn,"""select count(*) as count_no from bi_main.""" + table).iloc[0]['count_no']
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Truncate table')
    query.run_sql(clcnn, """truncate table bi_main.""" + table)
    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    tracking_time['LEN_TRUNCATE'] = len_truncate
    tracking_time['DURATION_TRUNCATE'] = int(second_process)

    # Insert:
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Insert table')

    query.df_to_mysqltable(clcnn, df, [], 'bi_main', table, False)


    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    tracking_time['DURATION_INSERT'] = int(second_process)
    tracking_time['TOTAL_DURATION'] = tracking_time['DURATION_GET'] + tracking_time['DURATION_TRUNCATE'] + tracking_time['DURATION_INSERT']

    tracking_time['UPDATE_TIME'] = datetime.datetime.now()
    tracking_time['TASK_ID'] = task_id
    query.json_to_db(biicnn, tracking_time, [], 'SYSTEM', 'R28_TRACKING_TIME', False)

    print(f'\t - {new_time} - {tracking_time["TOTAL_DURATION"]} - len insert: {tracking_time["LEN_INSERT"]} - len truncate  {tracking_time["LEN_TRUNCATE"]} \n')

    

def sync_table_2(table, key_col, batch_col,task_id):
    print(table)
    tracking_time = {}
    tracking_time['TABLE_NAME'] = table
    # Get table
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Get table')
    df = query.query(biicnn,
                     """select * from SYSTEM."""+table+"""
                        where ("""+key_col+""", """+batch_col+""") in 
                            (SELECT """+key_col+""", max("""+batch_col+""") 
                            FROM SYSTEM."""+table+""" group by order_id)""")
    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    tracking_time['DURATION_GET'] = int(second_process)
    tracking_time['LEN_INSERT'] = len(df)

    # Truncate table
    # len_truncate = query.query(clcnn,"""select count(*) as count_no from bi_main.""" + table).iloc[0]['count_no']
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Truncate table')
    # query.run_sql(clcnn, """truncate table bi_main.""" + table)
    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    # tracking_time['LEN_TRUNCATE'] = len_truncate
    tracking_time['DURATION_TRUNCATE'] = int(second_process)

    # Insert:
    old_time = datetime.datetime.now()
    print(f'\t\t\t{old_time} - Insert table')
    # query.df_to_mysqltable(clcnn, df, [], 'bi_main', table, False)
    new_time = datetime.datetime.now()
    time_delta = new_time - old_time
    second_process = max(time_delta.days*(3600*24) + time_delta.seconds,0)
    tracking_time['DURATION_INSERT'] = int(second_process)
    tracking_time['TOTAL_DURATION'] = tracking_time['DURATION_GET'] + tracking_time['DURATION_TRUNCATE'] + tracking_time['DURATION_INSERT']

    tracking_time['UPDATE_TIME'] = datetime.datetime.now()
    tracking_time['TASK_ID'] = task_id
    query.json_to_db(biicnn, tracking_time, [], 'SYSTEM', 'R28_TRACKING_TIME', False)

    print(f'\t - {new_time} - {tracking_time["TOTAL_DURATION"]} - len insert: {tracking_time["LEN_INSERT"]} - len truncate  {tracking_time["LEN_TRUNCATE"]} \n')
    
    
    
        
    