import sys
import os
from os.path import expanduser, join, abspath
import traceback

import datetime
import time
import csv
import re
from random import shuffle
import pandas as pd
import pickle
import numpy as np
import pyspark
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from os.path import expanduser, join, abspath
import functools
from impala.dbapi import connect
import multiprocessing as mp
from multiprocessing import Queue

# -- Define function for log printing and saving
# -- From:
# -- To: Function:module_logger.error(),
#        Function:module_logger.info()
# -- Usage: Log printing and saving
# -- Note:

import logging
# create logger 
logger = logging.getLogger('dataflow')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler(f'./dataflow.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(process)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

############################### Config ##########################
config = {}
config['database'] = 'temp'
config['time_period'] = """ date_key >= '20170101' and date_key < '20180101' """
config['starting_date'] = 20170101
config['ending_date'] = 20190729
###############################  End  ##########################


def impalaexec(sql,create_table=False):
    """
    execute sql using impala
    """
    '''
    execute sql by impala
    '''
    logger.info(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database']) as conn:
        curr = conn.cursor()
        curr.execute(sql)


def hiveexec(sql,create_table=False):
    """
    execute sql using hive
    """
    logger.info(sql)
    with connect(host='dtla1apps11', port=10000, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database']) as conn:
        curr = conn.cursor()
        curr.execute(sql)


def execute_impala_by_sql_file(table_name,file_path,set_timeperiod=False,database='config',dropfirst=True):
    """
    execute_impala_by_sql_file
    """
    if database == 'config':
        database = config['database']
    if dropfirst:
        sql = f""" drop table if exists {database}.{table_name} """
        impalaexec(sql)
        sql = f""" drop view if exists {database}.{table_name} """
        impalaexec(sql)
        
    with open(f'{file_path}') as f:
        sql = f.read()
    if set_timeperiod:
        sql = sql.format(database=database,starting_date=config['starting_date'],ending_date=config['ending_date'])
    else:
        sql = sql.format(database=database)
    impalaexec(sql)
    sql = f""" INVALIDATE METADATA {database}.{table_name} """
    impalaexec(sql)


def execute_hive_by_sql_file(table_name,file_path,set_timeperiod=False,database='config',dropfirst=True):
    """
    execute_hive_by_sql_file
    """
    if database == 'config':
        database = config['database']
    if dropfirst:
        sql = f""" drop table if exists {database}.{table_name} """
        hiveexec(sql)
        sql = f""" drop view if exists {database}.{table_name} """
        hiveexec(sql)
        
    with open(f'{file_path}') as f:
        sql = f.read()
    if set_timeperiod:
        sql = sql.format(database=database,starting_date=config['starting_date'],ending_date=config['ending_date'])
    else:
        sql = sql.format(database=database)
    hiveexec(sql)
    sql = f""" INVALIDATE METADATA {database}.{table_name} """
    impalaexec(sql)
    

#1
execute_impala_by_sql_file('forecast_store_code_scope_sprint4',\
                           '../sqls/1.forecast_store_code_scope_sprint4.sql')
#2
execute_impala_by_sql_file('forecast_itemid_list_threebrands_sprint4',\
                           '../sqls/2.forecast_itemid_list_threebrands_sprint4.sql', set_timeperiod=True)
#3
execute_impala_by_sql_file('forecast_item_id_family_codes_sprint4',\
                           '../sqls/3.vartefact.forecast_item_id_family_codes_sprint4.sql')
#4
execute_impala_by_sql_file('lastest_active_status',\
                           '../sqls/4.lastest_active_status.sql', set_timeperiod=True)
#5
execute_impala_by_sql_file('forecast_dm_plans_sprint4',\
                           '../sqls/5.forecast_dm_plans_sprint4.sql', set_timeperiod=True)
#6
execute_impala_by_sql_file('forecast_add_future_dms_sprint4',\
                           '../sqls/6.forecast_add_future_dms_sprint4.sql', set_timeperiod=True)
#7
execute_impala_by_sql_file('forecast_next_dm_sprint4',\
                           '../sqls/7.forecast_next_dm_sprint4.sql')
#8
execute_impala_by_sql_file('forecast_trxn_v7_sprint4',\
                           '../sqls/8.forecast_trxn_v7_sprint4.sql', set_timeperiod=True)
#9.0
execute_impala_by_sql_file('forecast_trxn_v7_full_item_id_sprint4',\
                           '../sqls/9.0forecast_trxn_v7_full_item_id_sprint4.sql')

#9.1
# 9.1  Don't know how to do it 
# 手动在hue里面加进去的
# os.system()

#9.2
execute_hive_by_sql_file('art_filter_non_promo',\
                           '../sqls/9.2art_filter_non_promo.sql')
#9.3
execute_hive_by_sql_file('art_filter_promo',\
                           '../sqls/9.3art_filter_promo.sql')
#9.4
execute_impala_by_sql_file('grouped_to_be_shipment',\
                           '../sqls/9.4grouped_to_be_shipment.sql')
#9.5
execute_impala_by_sql_file('p4cm_item_map_complete',\
                           '../sqls/9.5p4cm_item_map_complete.sql', set_timeperiod=True)
#9.6
execute_impala_by_sql_file('shipment_scope_map_corrected',\
                           '../sqls/9.6shipment_scope_map_corrected.sql', set_timeperiod=True)

#9.7 是一个python文件 
# 我先复制到temp中一个

#9.8
execute_impala_by_sql_file('forecast_item_store_perc_flagged',\
                           '../sqls/9.8forecast_item_store_perc_flagged.sql')
#9.9
execute_impala_by_sql_file('forecast_trxn_flag_v1_sprint4',\
                           '../sqls/9.9forecast_trxn_flag_v1_sprint4.sql')
#10
execute_impala_by_sql_file('forecast_sprint4_full_date_daily_sales',\
                           '../sqls/10.forecast_sprint4_full_date_daily_sales.sql', set_timeperiod=True)
#11
execute_impala_by_sql_file('forecast_sprint4_out_of_stock_median',\
                           '../sqls/11.out_of_stock_median_final.sql')
#12.1
execute_impala_by_sql_file('forecast_sprint4_trxn_to_day',\
                           '../sqls/12.1forecast_sprint4_trxn_to_day.sql')
#12.2
execute_impala_by_sql_file('forecast_sprint4_daily_future_dms',\
                           '../sqls/12.2forecast_sprint4_daily_future_dms.sql')
#12.3
execute_impala_by_sql_file('forecast_sprint4_daily_next_dm',\
                           '../sqls/12.3forecast_sprint4_daily_next_dms.sql')
#12.4
execute_impala_by_sql_file('forecast_sprint4_add_dm_to_daily',\
                          '../sqls/12.4forecast_sprint4_add_dm_to_daily.sql')
#12.5
execute_impala_by_sql_file('forecast_sprint4_day_to_week',\
                           '../sqls/12.5forecast_sprint4_day_to_week.sql', set_timeperiod=True)
#12.6
execute_impala_by_sql_file('forecast_sprint4_day_to_week_test',\
                           '../sqls/12.6forecast_sprint4_day_to_week_test.sql')
#13 ?   
#14                     
execute_impala_by_sql_file('forecast_sprint2_festival_ticket_count_flag',\
                           '../sqls/14.forecast_sprint2_festival_ticket_count.sql')
#15
execute_impala_by_sql_file('forecast_sprint2_trxn_week_features_flag_sprint4',\
                           '../sqls/15.forecast_sprint2_trxn_week_features_flag_sprint4.sql', set_timeperiod=True)
#16
execute_impala_by_sql_file('forecast_sprint2_final_flag_sprint4',\
                           '../sqls/16.forecast_sprint2_final_flag_sprint4.sql')
#17
execute_impala_by_sql_file('forecast_assortment_full',\
                           '../sqls/17.forecast_assortment_full.sql', set_timeperiod=True)
#18
execute_impala_by_sql_file('coupon_mapping',\
                           '../sqls/18.coupon_mapping.sql')
#19
execute_impala_by_sql_file('forecast_sprint3_v3_flag_sprint4',\
                           '../sqls/19.forecast_sprint3_v3_flag_sprint4.sql')
#20
execute_impala_by_sql_file('forecast_sprint3_coupon_item_link_flag_sprint4',\
                           '../sqls/20.forecast_sprint3_coupon_item_link_flag_sprint4.sql', set_timeperiod=True)
#21
execute_impala_by_sql_file('coupon_city_store_union_flag_sprint4',\
                           '../sqls/21.coupon_city_store_union_flag_sprint4.sql')
#22
execute_impala_by_sql_file('forecast_sprint3_v5_flag_sprint4',\
                           '../sqls/22.forecast_sprint3_v5_flag_sprint4.sql')
#23
execute_impala_by_sql_file('forecast_sprint3_v6_flag_sprint4',\
                           '../sqls/23.forecast_sprint3_v6_flag_sprint4.sql')
#24
execute_impala_by_sql_file('forecast_sprint3_v9_flag_sprint4',\
                           '../sqls/24.forecast_sprint3_v9_flag_sprint4.sql')
#25
execute_impala_by_sql_file('forecast_out_of_stock_temp',\
                           '../sqls/25.forecast_out_of_stock_temp.sql', set_timeperiod=True)
#26
execute_impala_by_sql_file('forecast_sprint3_v10_flag_sprint4',\
                           '../sqls/26.forecast_sprint3_v10_flag_sprin4.sql')

# New item 
#1 
execute_impala_by_sql_file('forecast_item_start_date',\
                           '../sqls/NEW_ITEMS/1_forecast_item_start_date.sql', set_timeperiod=True)
#2 
execute_impala_by_sql_file('forecast_sales_subfamily_store_date',\
                           '../sqls/NEW_ITEMS/2_forecast_sales_subfamily_store_date.sql')
#3 
execute_impala_by_sql_file('forecast_sales_subfamily_store_brand_date',\
                           '../sqls/NEW_ITEMS/3_forecast_sales_subfamily_store_brand_date.sql')

# DM dataset 
# 在做着部分之前 一些excel是要先传进去的 
# temp.dm_mapping_1719_dates_last_version
# temp.chinese_festival

#1
execute_impala_by_sql_file('forecast_sprint4_dm_agg_v2',\
                           '../sqls/PROMO/1_forecast_sprint4_dm_agg_v2.sql', set_timeperiod=True)

#2
execute_impala_by_sql_file('forecast_sprint4_promo_past_features',\
                           '../sqls/PROMO/2_forecast_sprint4_promo_past_features.sql', set_timeperiod=True)
#3
execute_impala_by_sql_file('forecast_sprint4_festival_lunar_feat',\
                           '../sqls/PROMO/3_forecast_sprint4_festival_lunar_feat.sql', set_timeperiod=True)
#4
execute_impala_by_sql_file('last_year_dm_sales_vrai_exact',\
                           '../sqls/PROMO/4_last_year_dm_sales_vrai_exact.sql', set_timeperiod=True)

# HiveServer2Error: AnalysisException: 
# Could not resolve table reference: 'temp.dm_mapping_1719_dates_last_version' 

#5 
execute_impala_by_sql_file('promo_dataset_feat_combine_exact_vrai',\
                           '../sqls/PROMO/5_promo_dataset_feat_combine_exact_vrai.sql', set_timeperiod=True)

#6 
execute_impala_by_sql_file('uplift_promo',\
                           '../sqls/PROMO/6_uplift_promo.sql', set_timeperiod=True)

#7
execute_impala_by_sql_file('forecast_sprint4_promo_uplift',\
                           '../sqls/PROMO/7_forecast_sprint4_promo_uplift.sql', set_timeperiod=True)



#8
execute_impala_by_sql_file('forecast_sprint4_promo_with_baseline',\
                           '../sqls/PROMO/8_forecast_sprint4_promo_with_baseline.sql', set_timeperiod=True)
#9
execute_impala_by_sql_file('forecast_sprint4_promo_with_coupon',\
                           '../sqls/PROMO/9_forecast_sprint4_promo_with_coupon.sql', set_timeperiod=True)

#10 
execute_impala_by_sql_file('forecast_sprint4_promo_mecha_v4',\
                           '../sqls/PROMO/10_forecast_sprint4_promo_mecha_v4.sql', set_timeperiod=True)









# After getting the results: Split week to day 
# OK 
# #1 
# # 1_2018_big_event_impact.ipynb 

#2 
execute_impala_by_sql_file('forecast_regular_day',\
                           '../sqls/PRED_TO_DAY/2_1forecast_regular_day.sql', set_timeperiod=True)

#3
execute_impala_by_sql_file('forecast_w2d_good_regular_days',\
                           '../sqls/PRED_TO_DAY/2_2forecast_w2d_good_regular_days.sql', set_timeperiod=True)

#4
execute_impala_by_sql_file('forecast_regular_dayofweek_percentage',\
                           '../sqls/PRED_TO_DAY/2_3forecast_regular_dayofweek_percentage.sql', set_timeperiod=True)

#5  
execute_impala_by_sql_file('forecast_regular_results_week_to_day_original_pred',\
                           '../sqls/PRED_TO_DAY/2_4forecast_regular_results_week_to_day_original_pred.sql', set_timeperiod=True)



# 

# After getting the results: Split DM to day 
# #0  
# # Upload DM pattern 

#1 
execute_impala_by_sql_file('dm_week_to_day_intermediate',\
                           '../sqls/PRED_TO_DAY/3_1dm_week_to_day_intermediate.sql', set_timeperiod=True)
#2
execute_impala_by_sql_file('dm_daily_sales',\
                           '../sqls/PRED_TO_DAY/3_2dm_daily_sales.sql', set_timeperiod=True)
#3
execute_impala_by_sql_file('dm_pattern_percentage',\
                           '../sqls/PRED_TO_DAY/3_3dm_pattern_percentage.sql', set_timeperiod=True)
#4
execute_impala_by_sql_file('subfamily_store_weekday_percentage',\
                           '../sqls/PRED_TO_DAY/3_4subfamily_store_weekday_percentage.sql', 
                           set_timeperiod=True)
#5
execute_impala_by_sql_file('forecast_DM_results_to_day',\
                           '../sqls/PRED_TO_DAY/3_5forecast_DM_results_to_day.sql', 
                           set_timeperiod=True)