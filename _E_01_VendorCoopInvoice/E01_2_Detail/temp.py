import sys

import pandas as pd
from support_tool import R0028_spt as sync
sys.path.insert(
	0, 'D:\Vendor_data_collect')
import sys
sys.path.append('D:\Vendor_data_collect')

er_info = {'MOTHER_TASK_ID': 11,
           'MOTHER_TASK_CONTENT': 'E01_COOPINVOICE',
           'MODULE_ID' : 'E01_3_2',
           'MODULE_CONTENT': 'RUN CL DETAIL'}

er_info['PHASE_NOTE'] = '1'
try:
    from support_tool.Connection import query as query
    from support_tool.Connection import Connect_y4abii as biicnn
    from support_tool.Connection import Connect_y4a_int as bii_int
    from time import sleep
except:
    e = sys.exc_info()
    print(f'{e[0]} - ')
    print(f'{e[1]} - ')
    print(f'{e[2]} - ')
    sleep(20)

cancel_condition = True
## insert data 144
tables_list = [['E01_COIN_PROMOTION_MASTER', 'Y4A_DWB_AMZ_COP_PRO'],
               ['E01_COIN_CLIP_MASTER', 'Y4A_DWB_AMZ_COP_CLP'],
               ['E01_COIN_CSF_MASTER', 'Y4A_DWB_AMZ_COP_CSF'],
               ['E01_COIN_OVERALL_MASTER', 'Y4A_DWB_AMZ_COP_OVR'],
               ['E01_COIN_SHORTAGE_MASTER', 'Y4A_DWB_AMZ_COP_STR'],
               ['E01_ORI_REDEMTION_MASTER','Y4A_DWB_AMZ_COP_RDT']]
for table in tables_list:
    ori_table = table[0]
    des_table = table[1]
    sql = f"select * from SYSTEM.{ori_table}"
    df = query.query(biicnn, sql)
    df['COUNTRY'] = 'USA'
    query.run_sql(bii_int, f"delete from  Y4A_INT.{des_table} where country ='USA'")
    query.df_to_db(bii_int, df, [], 'Y4A_INT', des_table, True)