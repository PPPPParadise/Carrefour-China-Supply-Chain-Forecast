# -*- coding: utf-8 -*-

import datetime
import os
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.append(os.getcwd())
from importlib import reload
import utils_v2
reload(utils_v2)
from utils_v2 import read_data

warnings.filterwarnings('ignore')
pd.set_option('mode.use_inf_as_na', True)


def preprocess(folder, big_table, sql_table, target_value, dataset_name):
    """[preprocess the data to use the weekly model]

    Arguments:
        folder {[sring]} -- [Folder containing the dataset]
        big_table {[string]} -- [name of the table]
        sql_table {[string]} -- [name of the corresponding table in the datalake]
        target_value {[string]} -- [column name of the target value]
        dataset_name {[string]} -- [name of the output data table]

    Raises:
        Exception: [If features are duplicated]
        Exception: [If some features are duplicated]
        Exception: [If there are duplicated in the features nameß]

    Returns:
        [type] -- [description]
    """

    table_path = folder + big_table

    table_df = pd.read_csv(table_path)

    table_df['full_item'] = table_df['item_id'].astype(str) + '_' + table_df['sub_id'].astype(str)
    table_df['full_item'] = table_df['item_id'].astype(str) + '_' + table_df['sub_id'].astype(str)
    table_df['item_store'] = table_df['full_item'] + '_' + table_df['store_code'].astype(str)


    # ### Load and preprocess

    table_df, errors, liste = read_data(table_df, sql_table)

    # ## Clean data and quick analysis

    table_df.current_dm_psp_start_date = pd.to_datetime(table_df.current_dm_psp_start_date)
    table_df.current_dm_psp_end_date = pd.to_datetime(table_df.current_dm_psp_end_date)
    table_df.next_dm_start_date = pd.to_datetime(table_df.next_dm_start_date)
    table_df.next_dm_end_date = pd.to_datetime(table_df.next_dm_end_date)

    table_df.loc[table_df.current_dm_psp_start_date.notnull(), 'current_dm_busday'] = np.busday_count(
        table_df.current_dm_psp_start_date.dropna().values.astype('datetime64[D]'),
        table_df.current_dm_psp_end_date.dropna().values.astype('datetime64[D]'))

    # number weekend in current DM
    table_df['current_dm_weekend_days'] = table_df.curr_psp_days - table_df.current_dm_busday

    # next_dm_busday
    table_df.loc[table_df.next_dm_start_date.notnull(), 'next_dm_busday'] = np.busday_count(
        table_df.next_dm_start_date.dropna().values.astype('datetime64[D]'),
        table_df.next_dm_end_date.dropna().values.astype('datetime64[D]'))
    
    # number weekend in next DM
    table_df['next_dm_weekend_days'] = table_df.next_dm_days - table_df.next_dm_busday

    # ### Adding BP feature
    try:
        table_df['bp_flag']

        table_df = table_df.sort_values(
            ['item_id', 'sub_id', 'store_code', 'week_key']).reset_index(drop=True)
        ordered = table_df.groupby(['item_id', 'sub_id', 'store_code', 'week_key'])[
            'bp_flag'].sum().reset_index()

        ordered['rolling_sum'] = ordered['bp_flag'].shift(1).rolling(
            52, min_periods=0).sum()

        ordered['rolling_count'] = ordered['bp_flag'].shift(1).rolling(
            52, min_periods=0).count()

        ordered['item_store'] = table_df['item_id'].astype(str) + '_' +\
            table_df['sub_id'].astype(str) + '_' + table_df['store_code'].astype(str)

        missings = table_df[~table_df.item_store.isin(ordered.item_store)]
        missing_bp = missings.groupby(['item_id', 'sub_id', 'store_code', 'week_key'])[
            'bp_flag'].sum().reset_index()

        missing_bp['rolling_sum'] = missing_bp['bp_flag'].shift(1).rolling(
            52, min_periods=0).sum()

        missing_bp['rolling_count'] = missing_bp['bp_flag'].shift(1).rolling(
            52, min_periods=0).count()

        total = pd.concat([ordered, missing_bp])

        table_df = table_df.merge(total, how="left")

        table_df["bp_ratio"] = table_df['rolling_sum'] / table_df['rolling_count']

    except:
        print('NO BP FLAG')

    # ### Preparing data for xgboost

    identification_features = [
        'full_item',
        'item_store',
        'week_key',
        'family_edesc',
        'family_ldesc',
        'group_family_edesc',
        'item_id',
        'itmcapa',
        'itmcaput',
        'itmedesc',
        'itmldesc',
        'itmpack',
        'itmstkun',
        'sub_code',
        'sub_family_edesc',
        'sub_family_ldesc',
        'sub_id',
        'week_begin_date',
        'week_end_date',
        'barcode',

        'current_dm_theme_id',
        'next_dm_theme_id',
        'out_stock_flag',

        # Deleted since no bps
        # 'planned_bp_flag',
        # 'bp_flag',

        'sales_amt_sum',
        # Deleted on 22/07 since not used anymore
        # 'sales_qty_sum_old',
        'sales_qty_sum',

        #ADDED in forecast_sprint3_v10_flag
        'ind_out_of_stock',

    ]

    flat_features = [
        'trxn_year',
        'trxn_month',

        # disapeared on vartefact.forecast_sprint3_v10_flag_sprint4
        # 'trxn_week',
        'current_dm_page_no',
        'current_dm_nsp',
        'current_dm_psp',
        'current_dm_msp',
        'ind_current_on_dm_flag',

        # add active flag as feature
        'active_flag',
        'assortment_active_flag',
        'holiday_count_new',

        'next_dm_psp',
        'next_dm_nsp',
        'next_dm_page_no',
        'next_dm_msp',
        'psp_nsp_ratio',

        'high_temp_month',
        'low_temp_month',


        'curr_psp_start_dow',
        'curr_psp_end_dow',
        'curr_psp_days',
        'curr_psp_start_week_count',
        'curr_psp_end_week_count',

        'next_dm_start_week_count',
        'next_dm_days',

        # Moved to dummies
        # 'festival_type',
        'last_year_festival_ticket_count',

        # DELETED since not working
        # 'last_year_sales_qty',

        #ADDED in forecast_sprint3_v10_flag
        'last_year_sales_qty_new',

        # ADDED since forecast_sprint3_v3 : from Python
        'current_dm_busday',
        'current_dm_weekend_days',
        'next_dm_busday',
        'next_dm_weekend_days',

        # ADDED since sprint3_v4: from SQL
        'ind_coupon_typeid_dd',
        'ind_coupon_typeid_np',
        'ind_coupon_typeid_ac',
        'ind_coupon_typeid_cc',
        'ind_coupon_typecode_cp',
        'ind_coupon_typecode_mpm',
        'ind_coupon_typecode_mp',
        'ind_coupon_typecode_other',
        'ndv_coupon_activity_type_id',
        'ndv_coupon_typecode',

        # ADDED since forecast_sprint3_v7 : from SQL
        'ndv_coupon_count1',
        'ind_coupon_count1_27',
        'ind_coupon_count1_02_25',
        'ind_coupon_count1_60',
        'ind_coupon_count1_4',
        'ind_coupon_count1_1',
        'ind_coupon_count1_58',
        'ind_coupon_count1_11',
        'ind_coupon_count1_16_34',
        'ind_coupon_count1_56',
        'ind_coupon_count1_37',
        'ind_coupon_count1_44',
        'ind_coupon_count1_9',
        'ind_coupon_count1_10',
        'ind_coupon_count1_other',
    ]

    time_features = [

        # renamed since forecast_sprint3_v3 : from Python
        'disc_ratio',
        'sales_qty_fam_wgt',
        'sales_qty_fam_store_wgt',

        'fam_sales_qty_mean_4w',
        'fam_sales_qty_mean_12w', 'fam_sales_qty_mean_24w',
        'fam_sales_qty_mean_52w', 'fam_sales_qty_min_roll',
        'fam_sales_qty_max_roll', 'subfam_sales_qty_mean_4w',
        'subfam_sales_qty_mean_12w', 'subfam_sales_qty_mean_24w',

        'subfam_sales_qty_mean_52w',
        'item_store_mean_4points', 'item_store_mean_12points',
        'item_store_mean_24points', 'item_store_mean_52points',
        'ind_competitor',

        'assortment_avg_nsp',
        # deleted since no bp in the dataset
        # "bp_ratio",

        # delete since using assortment_avg_nsp
        # 'ratio_amt_qty_new'

        # ADDED since forecast_sprint3_v6 : from SQL
        'fam_sales_item_ratio_4w',
        'fam_sales_item_ratio_12w',
        'fam_sales_item_ratio_24w',
        'fam_sales_item_ratio_52w',
        'subfam_sales_item_ratio_4w',
        'subfam_sales_item_ratio_12w',
        'subfam_sales_item_ratio_24w',
        'subfam_sales_item_ratio_52w',


        # ADDED since forecast_sprint3_v3 : from Python
        #         'family_code_sales_item_ratio_4w',
        #         'family_code_sales_item_ratio_12w',
        #         'family_code_sales_item_ratio_24w',
        #         'family_code_sales_item_ratio_52w',
        #         'group_family_code_sales_item_ratio_4w',
        #         'group_family_code_sales_item_ratio_12w',
        #         'group_family_code_sales_item_ratio_24w',
        #         'group_family_code_sales_item_ratio_52w',
        #         'sub_family_code_sales_item_ratio_4w',
        #         'sub_family_code_sales_item_ratio_12w',
        #         'sub_family_code_sales_item_ratio_24w',
        #         'sub_family_code_sales_item_ratio_52w',
        #         'family_code_sales_item_DC_ratio_4w',
        #         'family_code_sales_item_DC_ratio_12w',
        #         'family_code_sales_item_DC_ratio_24w',
        #         'family_code_sales_item_DC_ratio_52w',
        #         'group_family_code_sales_item_DC_ratio_4w',
        #         'group_family_code_sales_item_DC_ratio_12w',
        #         'group_family_code_sales_item_DC_ratio_24w',
        #         'group_family_code_sales_item_DC_ratio_52w',
        #         'sub_family_code_sales_item_DC_ratio_4w',
        #         'sub_family_code_sales_item_DC_ratio_12w',
        #         'sub_family_code_sales_item_DC_ratio_24w',
        #         'sub_family_code_sales_item_DC_ratio_52w',



    ]

    dummy_features = [
        'festival_type',

        'store_code',
        # 'city_code',
        # 'group_family_code',
        # 'family_code',
        'sub_family_code',
        'three_brand_item_list_holding_code',
        'current_dm_slot_type_code',
        'next_dm_slot_type_code',
        'next_dm_prom_type',

        # ADDED since forecast_sprint3_v3
        'item_seasonal_code',

        # booklet, leaflet, or DM
        'current_prom_type',

        # normal pages, member pages, seaosonal, private or trending, organic
        'current_dm_page_strategy',
        'next_dm_page_strategy'
    ]

    # # Check features missing

    used_cols = dummy_features + time_features + flat_features + identification_features

    used_cols_df = pd.Series(used_cols)
    error_do_not_exist = used_cols_df[~used_cols_df.isin(table_df.columns)]

    if not(error_do_not_exist.to_list() == []):
        print(error_do_not_exist)
        raise Exception('Some features do not exist. Check the names!')

    if not(len(used_cols) == len(set(used_cols))):
        print(used_cols_df[used_cols_df.duplicated()])
        raise Exception('Duplicated features!!!')

    def create_dummies(dummies_names, df):
        for i in dummies_names:
            df = pd.concat([df, pd.get_dummies(df[i], prefix=i+"_")], axis=1)
        return df

    table_df = create_dummies(dummy_features, table_df)

    dummies_features = []
    for i in table_df.columns:
        if any([i.startswith(s + '__') for s in dummy_features]):
            dummies_features.append(i)

    features = dummies_features + flat_features + time_features

    sample = table_df.loc[:50, features]

    if not(sample.columns[sample.columns.duplicated()].to_list() == []):
        print(sample.columns[sample.columns.duplicated()])
        raise Exception('Some features are duplicated. Check the names!')

    # #### Features engineering

    # ### Filter active version

    now = datetime.datetime.now().strftime("%m-%d_%H:%M:%S")
    str(now)

    names_features = [folder + 'features/dummies_features_' + str(now) + '.csv',
                      folder + 'features/flat_features_' + str(now) + '.csv',
                      folder + 'features/time_features_' + str(now) + '.csv',
                      folder + 'features/identification_' + str(now) + '.csv']

    try:
        os.mkdir(folder + 'features')
        print("Directory created ")
    except FileExistsError:
        print("Directory already exists")
        raise Exception('Folder already exists, probably contains features')

    pd.Series(dummies_features).to_csv(
        names_features[0], index=False, header=False)
    pd.Series(flat_features).to_csv(
        names_features[1], index=False, header=False)
    pd.Series(time_features).to_csv(
        names_features[2], index=False, header=False)
    pd.Series(identification_features).to_csv(
        names_features[3], index=False, header=False)


    table_df[target_value] = table_df[target_value].astype(float)

    # ### DELETE NEGATIVE SALES

    table_df = table_df[table_df['sales_qty_sum'] >= 0]

    # ## Change name of dataset

    df_noz = table_df[table_df['sales_qty_sum'] >= 0]

    df_noz['sales_qty_sum'].describe()

    mid = int(round(len(df_noz)/2, 0))

    import pickle
    with open(folder + dataset_name + '_part1'+'.pkl', 'wb') as output_file:
        pickle.dump(df_noz[:mid], output_file)

    import pickle
    with open(folder + dataset_name + '_part2'+'.pkl', 'wb') as output_file:
        pickle.dump(df_noz[mid:], output_file)


if __name__ == '__main__':
    """[Preprocess the data for weekly model]
    """

    # Define variables
    folder = '83.new_bp/'
    big_table = 'forecast_sprint3_v11_flag_sprint4.csv'
    sql_table = 'vartefact.forecast_sprint3_v11_flag_sprint4'
    target_value = 'sales_qty_sum'
    dataset_name = 'dataset_1307'

    preprocess(folder, big_table, sql_table,
               target_value, dataset_name)
