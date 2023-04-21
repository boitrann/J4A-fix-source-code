# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 05:20:17 2021

@author: Guest_1
"""




from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn    
import pandas as pd

df_list = pd.read_excel('D:\Chrome_driver\BOT_CENTER\BOT_CENTER.xlsx')


query.df_to_oratable(biicnn, df_list, ['ID'], 'BOT_CENTER_MANAGEMENT', False)



###############################################################################

from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn    
import pandas as pd

import os
computer_name =os.environ['COMPUTERNAME']
user_name = os.environ['USERNAME']

df_bot_list = query.query(biicnn, """select * from BOT_CENTER_MANAGEMENT
                          where computer_name = '""" + computer_name + """'
                          and user_name = '""" + user_name + """'""")
                          
                          
from login import openbrowser
from login import login

for index, row in df_bot_list.iterrows():
    driver = openbrowser.open_chrome( row['PROFILE_PATH'], 'D:/Chrome_driver/chromedriver.exe')
    login.amz_vendor_login(driver)
    print(row['DOWNLOAD_PATH'])
    break

    driver.close()
    driver.quit()
    df_bot_list = df_bot_list.drop(df_bot_list[df_bot_list['BOT_CODE']==row['BOT_CODE']].index)
    

# UPDATE BOT LIMIT BY COMPUTER
import os
df_bot_limit = pd.DataFrame([], columns = [])
bot_limit = {}
bot_limit['COMPUTER_NAME'] = os.environ['COMPUTERNAME']
bot_limit['LIMIT_ACCESS'] = 6
df_bot_limit = df_bot_limit.append(bot_limit, ignore_index = True)
query.df_to_oratable(biicnn, df_bot_limit, [], 'BOT_LIMIT_BY_COMPUTER', False)
 