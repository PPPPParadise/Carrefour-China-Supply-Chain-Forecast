# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.6
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import datetime
import os
##### part 2 Load and preprocess
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
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('max_colwidth', 200)
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

    df_test = pd.read_csv(table_path)

    df = df_test

    # df.columns.to_list()

    # len(df[df['out_stock_flag'] == 1])

    # len(df[df['ind_out_of_stock'] == 1])

    # len(df[df['out_stock_flag'] == 1]) / len(df)

    # len(df[df['ind_out_of_stock'] == 1]) / len(df)

    df['full_item'] = df['item_id'].astype(str) + '_' + df['sub_id'].astype(str)
    df['full_item'] = df['item_id'].astype(str) + '_' + df['sub_id'].astype(str)
    df['item_store'] = df['full_item'] + '_' + df['store_code'].astype(str)

    # len(df)

    # len(df[(df['out_stock_flag'] == 1) & (df['ind_current_on_dm_flag'] == 1)]) / len(df[
    #     df['ind_current_on_dm_flag'] == 1])

    # len(df[(df['out_stock_flag'] == 1) & (df['ind_current_on_dm_flag'] != 1)]) / len(df[
    #     df['ind_current_on_dm_flag'] != 1])

    # len(df[(df['ind_out_of_stock'] == 1) & (df['ind_current_on_dm_flag'] == 1)]) / len(df[
    #     df['ind_current_on_dm_flag'] == 1])

    # len(df[(df['ind_out_of_stock'] == 1) & (df['ind_current_on_dm_flag'] != 1)]) / len(df[
    #     df['ind_current_on_dm_flag'] != 1])

    # len(df[(df['ind_out_of_stock'] == 1) & (
    #     df['ind_current_on_dm_flag'] == 1)]) / len(df)

    # len(df[(df['ind_out_of_stock'] == 1) & (
    #     df['ind_current_on_dm_flag'] != 1)]) / len(df)

    # len(df[(df['week_key'] > 201901)
    #        & (df['assortment_active_flag'] == 1)])

    # len(df[(df['week_key'] > 201901)])

    # len(df[(df['week_key'] > 201901)
    #        & (df['assortment_active_flag'] == 1)]) / len(df[(df['week_key'] > 201901)])


    # ### Load and preprocess
    # with open('calendar.pkl', 'rb') as input_file:
    #     calendar = pickle.load(input_file)
    df2, errors, liste = read_data(df, sql_table)
    # errors
    # len(df2)

    df = df2
    # ## Clean data and quick analysis

    # len(df.loc[:, 'full_item'].unique())

    # len(df.loc[:, 'item_store'].unique())

    # +
    # Previously: 3038235 active flags
    # -

    # len(df.loc[df['active_flag'] == 1])

    # try:
    #     len(df.loc[df['bp_flag'] == 1])
    # except:
    #     print('NO BP FLAG')

    # len(df.loc[df['planned_bp_flag'] == 1])

    # + {"active": ""}
    # len(df.loc[(df['bp_flag'] == 0) | (df['planned_bp_flag'] == 0)])
    # -

    # len(df.loc[:, 'full_item'].unique())

    # + {"active": ""}
    # len(df.loc[df['iqr_trxn_flag'] == 1])

    # + {"active": ""}
    # df.loc[df['iqr_trxn_flag'] == 1, 'sales_qty_sum'].sum() / df.loc[:, 'sales_qty_sum'].sum()
    # -

    # + {"active": ""}
    # df.loc[df['iqr_trxn_flag'] == 1, 'sales_qty_sum_new'].sum() / df.loc[:, 'sales_qty_sum_new'].sum()
    # -

    # df['out_stock_flag'].unique()

    # +
    # Previously: 208664
    # -

    # len(df.loc[df['out_stock_flag'] == 1.])

    # len(df.loc[df['out_stock_flag'] == 0])

    # len(df[df['planned_bp_flag'] == 0])

    # len(df.loc[df['assortment_active_flag'] == 1])

    # # Adding extra features:

    # +
    # current_dm_busday
    df.current_dm_psp_start_date = pd.to_datetime(df.current_dm_psp_start_date)
    df.current_dm_psp_end_date = pd.to_datetime(df.current_dm_psp_end_date)
    df.next_dm_start_date = pd.to_datetime(df.next_dm_start_date)
    df.next_dm_end_date = pd.to_datetime(df.next_dm_end_date)

    df.loc[df.current_dm_psp_start_date.notnull(), 'current_dm_busday'] = np.busday_count(
        df.current_dm_psp_start_date.dropna().values.astype('datetime64[D]'),
        df.current_dm_psp_end_date.dropna().values.astype('datetime64[D]'))

    # number weekend in current DM
    df['current_dm_weekend_days'] = df.curr_psp_days - df.current_dm_busday

    # next_dm_busday
    df.loc[df.next_dm_start_date.notnull(), 'next_dm_busday'] = np.busday_count(
        df.next_dm_start_date.dropna().values.astype('datetime64[D]'),
        df.next_dm_end_date.dropna().values.astype('datetime64[D]'))
    
    # number weekend in next DM
    df['next_dm_weekend_days'] = df.next_dm_days - df.next_dm_busday

    # ### Adding BP feature
    try:
        df['bp_flag']

        df = df.sort_values(
            ['item_id', 'sub_id', 'store_code', 'week_key']).reset_index(drop=True)
        ordered = df.groupby(['item_id', 'sub_id', 'store_code', 'week_key'])[
            'bp_flag'].sum().reset_index()

        ordered['rolling_sum'] = ordered['bp_flag'].shift(1).rolling(
            52, min_periods=0).sum()

        ordered['rolling_count'] = ordered['bp_flag'].shift(1).rolling(
            52, min_periods=0).count()

        ordered['item_store'] = df['item_id'].astype(str) + '_' +\
            df['sub_id'].astype(str) + '_' + df['store_code'].astype(str)

        missings = df[~df.item_store.isin(ordered.item_store)]
        missing_bp = missings.groupby(['item_id', 'sub_id', 'store_code', 'week_key'])[
            'bp_flag'].sum().reset_index()

        missing_bp['rolling_sum'] = missing_bp['bp_flag'].shift(1).rolling(
            52, min_periods=0).sum()

        missing_bp['rolling_count'] = missing_bp['bp_flag'].shift(1).rolling(
            52, min_periods=0).count()

        total = pd.concat([ordered, missing_bp])

        len(total)

        df = df.merge(total, how="left")

        len(df)

        df["bp_ratio"] = df['rolling_sum'] / df['rolling_count']

    except:
        print('NO BP FLAG')
    # -

    # ### Preparing data for xgboost

    # +
    identification = [
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
    # -

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

    dummies_names = [
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

    used_cols = dummies_names + time_features + flat_features + identification

    # ok = df.columns[df.columns.isin(used_cols)]
    # not_used = df.columns[~df.columns.isin(used_cols)]
    # print('Columns not used:')
    # print(not_used.to_list())

    # + {"active": ""}
    # type id: 3 types
    #     01, 02, 04
    #     80% is 01, direct discount, dd
    #     17% 02, next purchase coupon, np
    #     04, changing coupon, cp
    #     --> we dont have 03, AC advertising coupon in our scope
    #
    # type code: 14 different
    #     7 in our table
    #     CP: customer purchase, 94%
    #     MPM: promotion for carrefour members, 2%
    #     MP: member price, 2%
    #     CC: customer changing coupon, less than 1%
    #     MPCM: member point used for items, 0.3%
    #     MSG: member point, less 0.2
    #     EX: exchange coupon, less 0.2
    #
    # NDV: Number of distinct value
    #   ndv_coupon_activity_type_id
    #   ndv_coupon_typecode
    #
    # -

    used_cols_df = pd.Series(used_cols)
    error_do_not_exist = used_cols_df[~used_cols_df.isin(df.columns)]

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

    df = create_dummies(dummies_names, df)

    dummies_features = []
    for i in df.columns:
        if any([i.startswith(s + '__') for s in dummies_names]):
            dummies_features.append(i)

    # dummies_names

    features = dummies_features + flat_features + time_features

    sample = df.loc[:50, features]

    sample.columns[sample.columns.duplicated()]

    if not(sample.columns[sample.columns.duplicated()].to_list() == []):
        print(sample.columns[sample.columns.duplicated()])
        raise Exception('Some features are duplicated. Check the names!')

    # #### Features engineering


    # len(set(flat_features))

    # len(set(time_features))

    # len(set(dummies_names))

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
    pd.Series(identification).to_csv(
        names_features[3], index=False, header=False)

    dummies_features = pd.read_csv(names_features[0], squeeze=True).tolist()
    flat_features = pd.read_csv(names_features[1], squeeze=True).tolist()
    time_features = pd.read_csv(names_features[2], squeeze=True).tolist()
    identification = pd.read_csv(names_features[3], squeeze=True).tolist()

    # +

    #df_filtered = df_filtered[df_filtered['planned_bp_flag'] != 1]
    df[target_value] = df[target_value].astype(float)

    # -

    # ### DELETE NEGATIVE SALES

    df = df[df['sales_qty_sum'] >= 0]

    # len(df)

    # ## Change name of dataset

    # len(df[df['sales_qty_sum'] == 0])

    df_noz = df[df['sales_qty_sum'] >= 0]

    # len(df)

    # len(df_noz)

    df_noz['sales_qty_sum'].describe()

    # + {"active": ""}
    # import pickle
    # with open(folder+ dataset_name + '_high_OOS_filled_med'+'.pkl', 'wb') as output_file:
    #     pickle.dump(df_filtered, output_file)
    # -

    mid = int(round(len(df_noz)/2, 0))
    # mid

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
