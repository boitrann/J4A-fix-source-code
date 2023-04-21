# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 10:16:28 2021

@author: Guest_1
"""


import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')

from support_tool.Connection import query as query
from support_tool.Connection import Connect_y4abii as biicnn 


from support_tool import support_tool as spt

from __Z_MAIN.Z_01_TASK_MANAGEMENT import Z01_spt as s_spt

import datetime


import os
import pandas as pd
import pytz

task_id = 1
# Gen task daily
s_spt.gen_daily_task(task_id)



# Get summary task
pt = pytz.timezone('US/Pacific')
s_spt.scan_request(datetime.datetime.now(tz = pt).replace(tzinfo = None), 2)
        
        
        
        
        
        




