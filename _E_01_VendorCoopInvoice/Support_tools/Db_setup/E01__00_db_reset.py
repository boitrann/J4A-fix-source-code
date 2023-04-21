# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 10:08:57 2021

@author: Guest_1
"""

# drop all table:
from _E_01_VendorCoopInvoice.Support_tools import E01_Main_Spt as o_spt
from _E_01_VendorCoopInvoice.Support_tools import E01_db_update as dup_spt
from _E_01_VendorCoopInvoice.Support_tools.Db_setup import E01_db_setup_mantb as man_setup
from _E_01_VendorCoopInvoice.Support_tools.Db_setup import E01_db_setup_datadb as dt_setup
from support_tool.Connection import Connect_y4abii as connection
import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')
db_info = o_spt.db_info()
#________________________________________________________________________________#
# REQ OVERALL 
# ora_table = db_info['OVR']['req']
# try:
#    dup_spt.drop_table(ora_table)
# except:
#     pass
# man_setup.create_table_reqman_collect_overall()
# man_setup.inport_ovr_req()
# ora_table = None


#________________________________________________________________________________#

# REQ DETAIL 
ora_table = db_info['DET']['req']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
man_setup.create_table_req_detail()
# Set ID from :
# dup_spt.reset_index_que(ora_table, 'ID', 34)
# ora_table = None


#________________________________________________________________________________#

# REQ detail list 
ora_table = db_info['DET']['req_detail']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
man_setup.create_table_req_detail_list()
ora_table = None


#________________________________________________________________________________#

# collected detail files man
ora_table = db_info['DET']['detailfile_man']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
man_setup.create_table_collecteddetail_files()
# man_setup.import_collected_detfile_man()# tạm thời bỏ qua
ora_table = None


#________________________________________________________________________________#

# Trantb_man
ora_table = db_info['DET']['trantb_man']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
man_setup.create_table_transittb_man()
# man_setup.import_tran_tb_man()#
ora_table = None


#________________________________________________________________________________#

# trancol_man
ora_table = db_info['DET']['trancol_man']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
man_setup.create_table_transitcols_man()
# man_setup.import_tran_col_man()
ora_table = None



#_____________________________________________________################________________________________________________#
from _E_01_VendorCoopInvoice.Support_tools.Db_setup import E01_db_setup_mantb as data_setup

migrate_path = r'D:\Download Management\Avc_CoopInvoices\Database\MIGRATION'
#________________________________________________________________________________#

# OVERALL FULLOG
ora_table = db_info['OVR']['fullog']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_overall_fullog()
# dt_setup.migrate_shortage_overall_fullog([ora_table])
ora_table = None


# SHORATGE FULLOG
ora_table = db_info['DET']['shortage_fullog']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_shortage_fullog()
# dt_setup.migrate_shortage_overall_fullog([ora_table])
ora_table = None


# OVERALL MASTER
ora_table = db_info['OVR']['master']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_overall_master()
# dt_setup.migrate_shortage_overall_master([ora_table])
ora_table = None


# SHORATGE FULLOG
ora_table = db_info['DET']['shortage_fullog']
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_shortage_master()
# dt_setup.migrate_shortage_overall_master([ora_table])
ora_table = None



#################################################################


# DEST_CLIP_fullog
ora_table = 'E01_COIN_CLIP_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_clipfee_fullog()

ora_table = None


#________________________________________________________________________________#

# DEST_CLIP_master
ora_table = 'E01_COIN_CLIP_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_clipfee_fullog()

ora_table = None


#________________________________________________________________________________#

# DEST_CLIP_master
ora_table = 'E01_COIN_CLIP_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_clipfee_master()

ora_table = None



#________________________________________________________________________________#

# DEST_promo_fullog
ora_table = 'E01_COIN_PROMOTION_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_pro_fullog()

ora_table = None


#________________________________________________________________________________#

# DEST_promo_master
ora_table = 'E01_COIN_PROMOTION_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_pro_master()

ora_table = None



#________________________________________________________________________________#

# DEST_csf_fullog
ora_table = 'E01_COIN_CSF_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_CSF_fullog()

ora_table = None


#________________________________________________________________________________#

# DEST_csf_master
ora_table = 'E01_COIN_CSF_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_dest_CSF_master()

ora_table = None


#________________________________________________________________________________#

# TRANSIT  CLIP_fullog
ora_table = 'E01_ORI_CLIP_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_clipfee_fullog()

ora_table = None


#________________________________________________________________________________#

# TRANSIT  CLIP_MASTER
ora_table = 'E01_ORI_CLIP_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_clipfee_master()

ora_table = None


#________________________________________________________________________________#

# TRANSIT  PRO T1 FULLOG
ora_table = 'E01_ORI_PROMOTION_T1_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_proT1_fullog()

ora_table = None

#________________________________________________________________________________#

# TRANSIT  PRO T1 MASTER
ora_table = 'E01_ORI_PROMOTION_T1_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_proT1_master()

ora_table = None


#________________________________________________________________________________#

# TRANSIT  PRO T2 FULLOG
ora_table = 'E01_ORI_PROMOTION_T2_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_proT2_fullog()

ora_table = None

#________________________________________________________________________________#

# TRANSIT  PRO T2 MASTER
ora_table = 'E01_ORI_PROMOTION_T2_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_proT2_master()

ora_table = None


#__________________________________________##########______________________________________#

# TRANSIT  CSF T1 FULLOG
ora_table = 'E01_ORI_CSF_T1_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT1_fullog()

ora_table = None

#________________________________________________________________________________#

# TRANSIT  CSF T1 MASTER
ora_table = 'E01_ORI_CSF_T1_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT1_master()

ora_table = None



#________________________________________________________________________________#

# TRANSIT  CSF T2 FULLOG
ora_table = 'E01_ORI_CSF_T2_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT2_fullog()

ora_table = None

#________________________________________________________________________________#

# TRANSIT  CSF T2 MASTER
ora_table = 'E01_ORI_CSF_T2_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT2_master()

ora_table = None


#________________________________________________________________________________#

# TRANSIT  CSF T3 FULLOG
ora_table = 'E01_ORI_CSF_T3_FULLOG'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT3_fullog()

ora_table = None

#________________________________________________________________________________#

# TRANSIT  CSF T3 MASTER
ora_table = 'E01_ORI_CSF_T3_MASTER'
try:
   dup_spt.drop_table(ora_table)
except:
    pass
dt_setup.create_table_transit_CSFT3_master()

ora_table = None








