# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 10:19:05 2021

@author: Guest_1
"""

"""
Quy trinh:
    - Tao task moi trong AA_TASK_LIST
    - Tao code Main
    - Tao bat loi trong sub module
    - Gan central bot: Assign bot, change bot profile info as bot reserve, release bot when done
    - Chinh sua code tuong tac DB
"""

import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')
from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn 


#from support_tool import support_tool as spt

from __Z_MAIN.Z_01_TASK_MANAGEMENT import Z01_spt as s_spt


import os
import pandas as pd
import datetime
import pytz

from time import sleep
     
# Gen task
def gen_new_task_sample():
    task_info = {}
    task_info['CONTENT'] = 'B02_NP'
    task_info['FREQUENCY'] = 6
    task_info['DAILY_STARTTIME_REQUIRED'] = 2
    task_info['DAILY_ENDTIME_REQUIRED'] = 22
    task_info['ACTIVE_STATUS'] = 'Active'
    
    df = pd.DataFrame([], columns = [])
    df = df.append(task_info, ignore_index = True)
    print('gen_new_task_sample')
    query.df_to_db(biicnn, df, ['ID'], 'SYSTEM', 'AA_TASK_LIST', False)
    
# Gen task daily
def gen_daily_task(task_id):
    #task_id = 1
    pt = pytz.timezone('US/Pacific')
    current_time = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    df_task_info = query.query(biicnn, """select * from SYSTEM.AA_TASK_LIST
                               where id = """ + str(task_id))
                               
    task_info = {}
    for index, row in df_task_info.iterrows():
        for col in df_task_info.columns:
            task_info[col] = row[col]
    # Check xem task đã hết h chạy cho 1 ngày hay chưa
    if current_time > datetime.datetime(current_time.year, current_time.month, current_time.day,task_info['DAILY_ENDTIME_REQUIRED'],0,0):
        review_date = datetime.datetime(current_time.year, current_time.month, current_time.day) +  datetime.timedelta(days=1)
    else:
        review_date = datetime.datetime(current_time.year, current_time.month, current_time.day)
    
    available = len(query.query(biicnn, """select * from SYSTEM.AA_TASK_DAILY_DETAIL 
                            where task_id = """ +  str(task_id) + """
                            and review_date = to_date('""" + review_date.strftime('%Y-%m-%d') + """','YYYY-MM-DD')
                            """) )
    
    if available ==0:
        # Gen request for review date
        left_times = task_info['FREQUENCY']
        if left_times>1:
            #số lần chạy trong ngày
            duration = (task_info['DAILY_ENDTIME_REQUIRED'] - task_info['DAILY_STARTTIME_REQUIRED'])/(task_info['FREQUENCY']-1)
        
        time_require = datetime.datetime(review_date.year, review_date.month, review_date.day, task_info['DAILY_STARTTIME_REQUIRED'],0,0)
        start_time = datetime.datetime(review_date.year, review_date.month, review_date.day, task_info['DAILY_STARTTIME_REQUIRED'],0,0)
        end_time = datetime.datetime(review_date.year, review_date.month, review_date.day, task_info['DAILY_ENDTIME_REQUIRED'],0,0)
                                                      
        while time_require >= start_time and time_require <= end_time and left_times >0 :
            #khởi tạo task daily detail
            new_dail_task = {}
            new_dail_task['REVIEW_DATE'] = review_date
            new_dail_task['TASK_ID'] = task_id
            new_dail_task['TIME_REQUIRED'] = time_require
            new_dail_task['STATUS'] = 'OPEN'

            df_new_daily_task = pd.DataFrame([], columns = [])
            df_new_daily_task = df_new_daily_task.append(new_dail_task, ignore_index = True)
            
            query.df_to_db(biicnn, df_new_daily_task, ['ID'], 'SYSTEM', 'AA_TASK_DAILY_DETAIL', False)
            
            left_times = left_times -1
            # tạo time cho batch kế
            if left_times>0:
                time_require = time_require +  datetime.timedelta(hours=duration)
            
      




def scan_request(current_time, task_id):
    ready_to_run = False
    while ready_to_run == False:
        # Cancel task miss time
        query.run_sql(biicnn, """update SYSTEM.AA_TASK_DAILY_DETAIL
                      set status = 'CANCEL' 
                      WHERE status IN ('OPEN')
                      and time_required < to_date('""" + current_time.strftime('%Y-%m-%d %H:%M:%S') 
                      + """','YYYY-MM-DD HH24:MI:SS')
                      and task_id = """ + str(task_id)
                      )
        query.run_sql(biicnn, """update SYSTEM.AA_TASK_DAILY_DETAIL
                        set status = 'OPEN'
                        WHERE STATUS IN ('RESERVED', 'ON_PROCESS')
                        and task_id = """ + str(task_id)
                      )
        # Scan daily task available:
        available = len(query.query(biicnn, """select * from SYSTEM.AA_TASK_DAILY_DETAIL 
                                where task_id = """ +  str(task_id) + """
                                and time_required >= to_date('""" + current_time.strftime('%Y-%m-%d %H:%M:%S') + """','YYYY-MM-DD HH24:MI:SS')
                                and status = 'OPEN'
                                """) )
    
        if available == 0:
            s_spt.gen_daily_task(task_id)
            
        else:
            # Get daily task_id
            df_daily_task = query.query(biicnn, """ select * from SYSTEM.AA_TASK_DAILY_DETAIL 
                                where task_id = """ +  str(task_id) + """
                                and time_required >= to_date('""" + current_time.strftime('%Y-%m-%d %H:%M:%S') + """','YYYY-MM-DD HH24:MI:SS')
                                and status = 'OPEN'
                                and time_required in (select min(time_required) from SYSTEM.AA_TASK_DAILY_DETAIL 
                                                    where task_id = """ +  str(task_id) + """
                                                    and time_required >= to_date('""" + current_time.strftime('%Y-%m-%d %H:%M:%S') +
                                                    """','YYYY-MM-DD HH24:MI:SS') and status = 'OPEN') 
                                and rownum = 1""")
            
            for index, row in df_daily_task.iterrows():
                task_info = {}
                for col in df_daily_task.columns:
                    task_info[col] = row[col]
                break
            
            ready_to_run = True
    return task_info


def lock_table_for_update(cnn, cursor, table_name):
    # table_name='BOT_CENTER_MANAGEMENT'
    # cnn,cursor = biicnn.connection()
    print(f'{datetime.datetime.now()} : Process Lock table for session')
    print()
    lock_status = False
    available_status = 'Null'
    
    sql_check_lock = "SELECT C.OWNER, C.OBJECT_NAME, C.OBJECT_TYPE, B.SID, B.SERIAL#,"
    sql_check_lock += "B.STATUS, B.OSUSER, B.MACHINE FROM V$LOCKED_OBJECT A, V$SESSION B,"
    sql_check_lock += "DBA_OBJECTS C WHERE B.SID = A.SESSION_ID AND A.OBJECT_ID = C.OBJECT_ID "
    sql_check_lock += "AND C.OBJECT_NAME = '" + table_name + "' AND C.OWNER = 'SYSTEM' "

    sql_lock = "select * from SYSTEM." + table_name + " for UPDATE"

    
    while lock_status == False:
        df_lock = pd.read_sql_query(sql_check_lock,cnn)
        if len(df_lock) == 0:
            cursor.execute(sql_lock)
        else:
            available_status = df_lock.iloc[0]['STATUS']
            if available_status == 'ACTIVE':
                lock_status = True
            else:
                sleep(0.5)
    print(f'\r -- {datetime.datetime.now()} : Availble Status {available_status} -- ', end = '\r')
    
    return lock_status, df_lock
            

def assign_bot_center(connection, task, req_id):
    assign_status = False
    # Get Limit by computer name
    computer_name =os.environ['COMPUTERNAME']
    user_name = os.environ['USERNAME']
    
    limit_by_compname = query.query(biicnn, """ select sum(LIMIT_ACCESS) as LIMIT_ACCESS 
                                    FROM SYSTEM.BOT_LIMIT_BY_COMPUTER
                                    WHERE COMPUTER_NAME = '""" + computer_name + """'""" ).iloc[0]['LIMIT_ACCESS']
    avai_bot_info = {}
    
    count_all =query.query(biicnn, """select count(*) as COUNT from SYSTEM.BOT_CENTER_MANAGEMENT
                                          where computer_name = '""" + computer_name + """'
                                          and user_name = '"""+ user_name + """'""").iloc[0]['COUNT']
    
    if limit_by_compname >0 and count_all >0:
        while assign_status == False:
            # LOCK TABLE FOR SESSION CHECK & UPDATE:
            cnn, cursor = connection.connection()
            #tạm thời bỏ qua
            #lock_status, df_lock = s_spt.lock_table_for_update(cnn, cursor, 'BOT_CENTER_MANAGEMENT')
            
            # Count on_process
            count_on_process = query.query(biicnn, """select count(*) as COUNT_ON_PROCESS from SYSTEM.BOT_CENTER_MANAGEMENT
                                          where computer_name = '""" + computer_name + """'
                                          and Status = 'On_process' """).iloc[0]['COUNT_ON_PROCESS']
            
            count_available =query.query(biicnn, """select count(*) as COUNT from SYSTEM.BOT_CENTER_MANAGEMENT
                                          where computer_name = '""" + computer_name + """'
                                          and user_name = '"""+ user_name + """'
                                          and Status = 'Available' """).iloc[0]['COUNT']
            print(count_available)
            # if available asign bot, if not release lock then comback 1 minutes later
            if count_available == 0 or count_on_process >= limit_by_compname:
                cnn.commit()
                cnn = None
                sleep(60)
            else:
                # Assign bot to task
                # Get available bot info
                df_bot_info = query.query(biicnn, """select * from SYSTEM.BOT_CENTER_MANAGEMENT
                                          where computer_name = '""" + computer_name + """'
                                          and user_name = '"""+ user_name + """'
                                          and Status = 'Available' 
                                          and rownum = 1""")
                
                for index, row in df_bot_info.iterrows():
                    for col in df_bot_info.columns:
                        avai_bot_info[col] = row[col]
                    break
                avai_bot_info['STATUS'] = 'On_process'
                cursor.execute("""update SYSTEM.BOT_CENTER_MANAGEMENT 
                               set status = 'On_process', 
                               task = '""" + task + """',
                               req_id = """ + str(req_id) + """,
                               update_time_vn = to_date('""" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                      + """','YYYY-MM-DD HH24:MI:SS')
                               where id = """ + str(avai_bot_info['ID']))
                cnn.commit()
                cnn = None
                assign_status = True
    else:
        if limit_by_compname == 0:
            print('Computer_name is not in list of assignment')
        elif count_all == 0:
            print('User name is not in list of assignment')
        
    return avai_bot_info, assign_status

def count_err(task_id):
    count_er = query.query(biicnn, """ select count(*) as COUNT from SYSTEM.AA_ERROR_LOG 
                              where status = 'NEW'
                              and mother_task_id = """ + str(task_id)).iloc[0]['COUNT']
    return count_er

def reserved_task(task_id):
    pt = pytz.timezone('US/Pacific')
    current_time = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    task_info = s_spt.scan_request(current_time, task_id)
    """
    update SYSTEM.AA_TASK_DAILY_DETAIL
    set status = 'CANCEL' 
    WHERE status IN ('OPEN')
    and time_required < to_date(current_time)
    and task_id = 11' 
    """

    """
    update SYSTEM.AA_TASK_DAILY_DETAIL
    set status = 'OPEN' 
    WHERE status IN ('RESERVED', 'ON_PROCESS')
    and task_id = 11' 
    """




    task_content = query.query(biicnn, """ select CONTENT from SYSTEM.AA_TASK_LIST 
                               where id = """ +str(task_id)).iloc[0]['CONTENT']
    print(f'\nTASK INFO: {task_content}')
    for key, value in task_info.items():
        if value != None:
            print(f'\t{key} : {task_info[key]}')
    
    time_delta = task_info['TIME_REQUIRED'] - datetime.datetime.now(tz = pt).replace(tzinfo = None)
    
    second_to_start = max(time_delta.days*(3600*24) + time_delta.seconds,0)   
    till_time = datetime.datetime.now() +  datetime.timedelta(seconds=second_to_start)
    print(f'\t- Time to start VNT Till: {till_time} \n') 
    
    # Reserved Task before sleep
    task_info['STATUS'] = 'RESERVED'
    update_status = query.json_update_to_db(biicnn, task_info, ['STATUS'], ['ID'], 'SYSTEM', 'AA_TASK_DAILY_DETAIL')
   
    # Sleep till start
    for i in range(second_to_start - 5):
        sleep(1)
        second_to_start = second_to_start - 1
    while second_to_start >0:
        time_delta = task_info['TIME_REQUIRED'] - datetime.datetime.now(tz = pt).replace(tzinfo = None)
        second_to_start = max(time_delta.days*(3600*24) + time_delta.seconds,0)  
        sleep(1)
    print()
    return update_status, task_info

def kick_off_task(task_info):
    pt = pytz.timezone('US/Pacific')
    task_info['STATUS'] = 'ON_PROCESS'
    task_info['START_TIME'] = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    update_status = query.json_update_to_db(biicnn, task_info, ['STATUS', 'START_TIME'], ['ID'],'SYSTEM', 'AA_TASK_DAILY_DETAIL')
    return update_status


def close_task(task_info):
    pt = pytz.timezone('US/Pacific')
    task_info['STATUS'] = 'COMPLETE'
    task_info['FINISH_TIME'] = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    time_delta_2 = task_info['FINISH_TIME'] - task_info['START_TIME']
    task_info['DURATION_HOURS'] = round(max(time_delta_2.days*(3600*24) + time_delta_2.seconds,0)/3600,2)
    update_status = query.json_update_to_db(biicnn, task_info, ['STATUS', 'FINISH_TIME', 'DURATION_HOURS'], ['ID'],
                                            'SYSTEM','AA_TASK_DAILY_DETAIL')
        
    return update_status

def clean_chrome_history(driver):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    import time
    
    driver.execute_script("window.open('');")
    time.sleep(2)
    driver.switch_to.window(driver.window_handles[-1])
    #time.sleep(2)
    driver.get('chrome://settings/clearBrowserData') # for old chromedriver versions use cleardriverData
    time.sleep(2)
    """
    actions = ActionChains(driver) 
    actions.send_keys(Keys.TAB * 2 + Keys.SPACE +  Keys.ENTER)
    actions.perform()
    time.sleep(2)
    
    actions = ActionChains(driver) 
    actions.send_keys(Keys.TAB * 2 + Keys.SPACE + Keys.TAB + Keys.SPACE)
    actions.perform()
    time.sleep(2)
    
    actions = ActionChains(driver) 
    actions.send_keys(Keys.TAB * 2  + Keys.ENTER)
    actions.perform()
    """
    actions = ActionChains(driver) 
    actions.send_keys(Keys.TAB * 7 + Keys.ENTER)
    actions.perform()
    
    time.sleep(5) # wait some time to finish
    driver.close() # close this tab
    driver.switch_to.window(driver.window_handles[0]) # switch back

def release_bot(bot_ID):
    query.run_sql(biicnn, """update SYSTEM.BOT_CENTER_MANAGEMENT 
                           set status = 'Available', 
                           task = Null,
                           req_id = Null,
                           update_time_vn = to_date('""" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                  + """','YYYY-MM-DD HH24:MI:SS')
                           where id = """ + str(bot_ID) )

def update_bot_log(bot_info, phase_note) :
    pt = pytz.timezone('US/Pacific')
    bot_info['UPDATE_TIME_VN'] = datetime.datetime.now()
    bot_info['UPDATE_TIME_PT'] = datetime.datetime.now(tz = pt).replace(tzinfo = None)
    bot_info['PHASE_NOTE'] = phase_note
    query.json_to_db(biicnn, bot_info, ['LOG_ID'], 'SYSTEM',  'BOT_MANAGEMENT_LOG', False)
    
    
    
    