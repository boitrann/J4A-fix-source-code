# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 10:31:51 2021

@author: Guest_1
"""


import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')

    


def db_info_new():
    db_info = {'ovr_rq':{'db': 'SYSTEM', 'tb':'E01_COIN_REQ_OVERALL'},
               'ovr_fl':{'db': 'SYSTEM', 'tb':'E01_COIN_OVERALL_FULLOG'},
               'ovr_mt':{'db': 'SYSTEM', 'tb':'E01_COIN_OVERALL_MASTER'},
               
               'dt_rq':{'db': 'SYSTEM', 'tb':'E01_COIN_REQ_DETAIL'},
               'dt_rql':{'db': 'SYSTEM', 'tb':'E01_COIN_REQ_DETAIL_LIST'},
               'st_fl':{'db': 'SYSTEM', 'tb':'E01_COIN_SHORTAGE_FULLOG'},
               'st_mt':{'db': 'SYSTEM', 'tb':'E01_COIN_SHORTAGE_MASTER'},
               'dt_fm':{'db': 'SYSTEM', 'tb':'E01_COIN_DETAIL_FILEMAN'},
               'dt_tb':{'db': 'SYSTEM', 'tb':'E01_COIN_TRANSITMAN_TABLES'},
               'dt_cl':{'db': 'SYSTEM', 'tb':'E01_COIN_TRANSITMAN_COLS'},
               
               'pr_fl':{'db': 'SYSTEM', 'tb':'E01_COIN_PROMOTION_FULLOG'},
               'pr_mt':{'db': 'SYSTEM', 'tb':'E01_COIN_PROMOTION_MASTER'},
               'np':{'db': 'SYSTEM', 'tb':'B02_NP_INFO_MASTER'},

               'clip_mt':{'db': 'SYSTEM', 'tb':'E01_COIN_CLIP_MASTER'},
               'clip_fl':{'db': 'SYSTEM', 'tb':'E01_COIN_CLIP_FULLOG'},
               
               'rdt_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_REDEMTION_MASTER'},
               'rdt_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_REDEMTION_FULLOG'},
               
               'csf_mt':{'db': 'SYSTEM', 'tb':'E01_COIN_CSF_MASTER'},
               'csf_fl':{'db': 'SYSTEM', 'tb':'E01_COIN_CSF_FULLOG'},
               
               'cp_adi_mt':{'db': 'SYSTEM', 'tb':'B01_CP_COUPON_ADDIN_MASTER'},
               
               'coop_vali':{'db': 'SYSTEM', 'tb':'B01_COOPINV_VALIDATION'},
               
               'clip_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_CLIP_FULLOG'},
               'clip_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_CLIP_MASTER'},
               
               'csf1_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T1_FULLOG'},
               'csf1_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T1_MASTER'},
               'csf2_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T2_FULLOG'},
               'csf2_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T2_MASTER'},
               'csf3_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T3_FULLOG'},
               'csf3_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_CSF_T3_MASTER'},

               'pr1_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_PROMOTION_T1_FULLOG'},
               'pr1_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_PROMOTION_T1_MASTER'},
               'pr2_o_fl':{'db': 'SYSTEM', 'tb':'E01_ORI_PROMOTION_T2_FULLOG'},
               'pr2_o_mt':{'db': 'SYSTEM', 'tb':'E01_ORI_PROMOTION_T2_MASTER'},

               'no_rec':{'db': 'SYSTEM', 'tb':'E01_NO_RECORD_DETAIL'}
               }
    for key, value in db_info.items():
        db_info[key]['full'] = db_info[key]['db'] + '.' + db_info[key]['tb']
    return db_info


def folder_config():
    folder_config = {'collected_path_ori': r'G:\.shortcut-targets-by-id\158bWWRdElFR4opqA795U9JLmcHs3CXXR\DE_DATA\USA\E01_VENDOR_COOP_INVOICES\OVR\ORI',
                 'collected_path_conv': r'G:\.shortcut-targets-by-id\158bWWRdElFR4opqA795U9JLmcHs3CXXR\DE_DATA\USA\E01_VENDOR_COOP_INVOICES\OVR\CONV',
                 'shortage_path': r'G:\.shortcut-targets-by-id\158bWWRdElFR4opqA795U9JLmcHs3CXXR\DE_DATA\USA\E01_VENDOR_COOP_INVOICES\SHORTAGE',
                 'detail_path': r'G:\.shortcut-targets-by-id\158bWWRdElFR4opqA795U9JLmcHs3CXXR\DE_DATA\USA\E01_VENDOR_COOP_INVOICES\DETAIL',
                 'pdf_path': r'G:\.shortcut-targets-by-id\158bWWRdElFR4opqA795U9JLmcHs3CXXR\DE_DATA\USA\E01_VENDOR_COOP_INVOICES\PDF',
                 }
    return folder_config    
    
    
def db_info():
    db_info = {
            'OVR' : 
                {'req' : 'E01_COIN_REQ_OVERALL',
                 'fullog': 'E01_COIN_OVERALL_FULLOG',
                 'master': 'E01_COIN_OVERALL_MASTER', 
                 'collected_path_ori': r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\OVR\ORI', 
                 'collected_path_conv': r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\OVR\CONV'},
            'DET':
                {'req' : 'E01_COIN_REQ_DETAIL', 
                 'req_detail' : 'E01_COIN_REQ_DETAIL_LIST', 
                 'shortage_fullog' : 'E01_COIN_SHORTAGE_FULLOG',
                 'shortage_master' : 'E01_COIN_SHORTAGE_MASTER',
                 'detailfile_man': 'E01_COIN_DETAIL_FILEMAN', 
                 'trantb_man' : 'E01_COIN_TRANSITMAN_TABLES', 
                 'trancol_man' : 'E01_COIN_TRANSITMAN_COLS', 
                 'shortage_path': r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\SHORTAGE', 
                 'detail_path': r'D:\Database\Vendor\E01_VENDOR_COOP_INVOICES\SHORTAGE'}
            } 
    return db_info
