import datetime
import glob
import os
import pickle
import sys
import warnings
from importlib import reload

import numpy as np
import pandas as pd
import utils_v2
from utils_v2 import read_data

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('max_colwidth', 200)
pd.set_option('mode.use_inf_as_na', True)


def preprocess_promo(folder, big_table, sql_table, target_value, dataset_name):
    """[Preprocess the saved dataset downloaded to the server for the promo model]

    Arguments:
        folder {[string]} -- [folder containing the dataset (Please put a "/" at the end of the name to indicate a folder)]
        big_table {[string]} -- [name of the dataset on the server]
        sql_table {[string]} -- [name of the corresponding table on the datalake]
        target_value {[string]} -- [name of the column to predict]

    Raises:
        Exception: [if there are missing features]
        Exception: [if there is duplicates in the features]
        Exception: [if the model cannot be trained properly]

    Returns:
        [type] -- [description]
    """

    table_path = folder + big_table

    table_df = pd.read_csv(table_path)

    table_df['current_dm_slot_type_code'].value_counts()

    try:
        print(len(table_df[table_df['ind_out_of_stock'] == 1]))
        print(len(table_df[table_df['ind_out_of_stock'] == 1]) / len(table_df))
    except:
        print('NO OOS flag')

    len(table_df)

    # ### Load and preprocess

    sys.path.append(os.getcwd())
    reload(utils_v2)

    table_df, errors, liste = read_data(table_df, sql_table)

    try:
        len(table_df.loc[table_df['active_flag'] == 1])
    except:
        print('NO ACTIVE FLAG')

    try:
        print(len(table_df.loc[table_df['planned_bp_flag'] == 1]))
        len(table_df[table_df['planned_bp_flag'] == 0])
    except:
        print('NO PLANNED BP FLAG')

    # +
    try:
        len(table_df.loc[table_df['bp_flag'] == 1])
    except:
        print('NO BP FLAG')

    try:
        len(table_df.loc[(table_df['bp_flag'] == 0) | (table_df['planned_bp_flag'] == 0)])
    except:
        print('NO BP FLAG or PLANNED BP FLAG')

    len(table_df.loc[:, 'full_item'].unique())

    try:
        table_df['ind_out_of_stock_flag'].unique()
    except:
        print('NO ind_out_of_stock_flag FLAG')

    try:
        len(table_df.loc[table_df['assortment_active_flag'] == 1])
    except:
        print('NO assortment_active_flag FLAG')

    # ### Need to drop lines with no dm end or start date infos

    table_df.current_dm_psp_start_date = pd.to_datetime(table_df.current_dm_psp_start_date)
    table_df.current_dm_psp_end_date = pd.to_datetime(table_df.current_dm_psp_end_date)

    df_dropped = table_df.dropna(
        subset=['current_dm_psp_start_date', 'current_dm_psp_end_date'], how='any')

    table_df = df_dropped

    table_df.loc[table_df.current_dm_psp_start_date.notnull(), 'current_dm_busday'] = np.busday_count(
        table_df.current_dm_psp_start_date.dropna().values.astype('datetime64[D]'),
        table_df.current_dm_psp_end_date.dropna().values.astype('datetime64[D]'))

    # number weekend in current DM
    table_df['curr_psp_days'] = (table_df.current_dm_psp_end_date
                           - table_df.current_dm_psp_start_date).dt.days

    table_df['current_dm_weekend_days'] = table_df.curr_psp_days - table_df.current_dm_busday

    # ## Adding uplift as feature

    table_df['uplift_value'] = table_df['4w_sales_4w_bef'] * table_df['uplift']

    table_df['uplift_value'].describe()

    # ### Preparing data for xgboost

    identification_features = [
        'item_id',
        'sub_id',

        # DELETED on 12/07 Sprint4: 95
        # 'dm_start_week',

        'dm_sales_qty',
        'current_dm_theme_id',
        'current_dm_theme_en_desc',

        # DELETED on 12/07 Sprint4: 95
        # 'current_theme_start_date',
        # 'current_theme_end_date',

        # ADDED back on 12/07 Sprint4: 96
        'current_theme_start_date',
        'current_theme_end_date',

        'current_dm_psp_start_date',
        'current_dm_psp_end_date',

        # DELETED on 12/07 Sprint4: 95
        # 'current_dm_msp_end_date',

        # ADDED on 12/07 Sprint4: 95
        'psp_start_week',
        'psp_start_month',
        'psp_end_week',

        'full_item',
        'item_store',

        # DELETED on 12/07 Sprint4: 95
        # 'ind_out_of_stock'
    ]

    flat_features = [
        # DELETED on 12/07 Sprint4: 95
        # 'trxn_month',
        # 'trxn_week',

        'current_dm_page_no',
        'current_dm_nsp',
        'current_dm_psp',

        # DELETED on 12/07 Sprint4: 95
        # 'psp_nsp_ratio',

        # ADDED on 12/07 Sprint4: 95
        'current_dm_psp_nsp_ratio',

        'last_year_dm_sales',
        'last_year_dm_psp_nsp_ratio',
        'last_year_fam_dm_sales_avg',
        'last_5dm_sales_avg',
        'fam_last_5dm_sales_avg',
        'current_dm_weekend_days',
        'curr_psp_days',

        # ADDED Sprint 4 v1:
        #          'last_year_lunar_sales_qty_1m_avg',
        #          'last_year_lunar_sales_qty_1m_sum',
        #          'last_year_lunar_ratio_1m',
        #          'last_year_lunar_sales_qty_3m_sum',
        #          'last_year_lunar_sales_qty_3m_avg',
        #          'last_year_lunar_ratio_3m',

        # ADDED Sprint 4: vrai exact
        'last_year_dm_sales_vrai_exact',
        'vrai_exact_or_lunar_1m',
        'vrai_exact_or_lunar_3m',
        'current_dm_busday',

        # ADDED Sprint 4: 93. uplift
        'uplift_value',
        'uplift',
        '4w_sales_4w_bef',

        # ADDED Sprint 4: 94. discount promo
        'coupon_disc_ratio_avg_max',
        'coup_disc_ratio_mech_max',

    ]

    time_features = [

    ]

    dummy_features = [
        'store_code',
        'item_seasonal_code',
        'sub_family_code',
        'current_dm_page_strategy_code',

        # DELETED on 12/07 Sprint4: 95
        # 'current_dm_nl',
        # ADDED on 12/07 Sprint4: 95
        'nl',

        'current_dm_slot_type_code',

        # ADDED Sprint 4 v1:
        'festival_type',

    ]
    # -

    table_df[[
        'nl',
        'current_dm_psp_nsp_ratio',
        'current_dm_slot_type_name',
        'family_code',
        'psp_start_week',
        'psp_start_month',
        'psp_end_week',
    ]].head()

    # # Check features missing

    used_cols = dummy_features + time_features + flat_features + identification_features

    not_used = table_df.columns[~table_df.columns.isin(used_cols)]
    not_used.to_list()

    used_cols_df = pd.Series(used_cols)
    error_do_not_exist = used_cols_df[~used_cols_df.isin(table_df.columns)]

    if not (error_do_not_exist.to_list() == []):
        print(error_do_not_exist)
        raise Exception('Some features do not exist. Check the names!')

    if not (len(used_cols) == len(set(used_cols))):
        print(used_cols_df[used_cols_df.duplicated()])
        raise Exception('Duplicated features!!!')

    def create_dummies(dummies_names, df):
        for i in dummies_names:
            df = pd.concat([df, pd.get_dummies(df[i], prefix=i + "_")], axis=1)

        return df

    table_df = create_dummies(dummy_features, table_df)

    dummies_features = []
    for i in table_df.columns:
        if any([i.startswith(s + '__') for s in dummy_features]):
            dummies_features.append(i)

    dummy_features

    features = dummies_features + flat_features + time_features

    sample = table_df.loc[:50, features]

    sample.columns[sample.columns.duplicated()]

    if not (sample.columns[sample.columns.duplicated()].to_list() == []):
        print(sample.columns[sample.columns.duplicated()])
        raise Exception('Some features are duplicated. Check the names!')

    #### Save features

    now = datetime.datetime.now()
    str(now)

    names_features = [folder + 'features/dummies_features_' + str(now) + '.csv',
                      folder + 'features/flat_features_' + str(now) + '.csv',
                      folder + 'features/time_features_' + str(now) + '.csv',
                      folder + 'features/identification_' + str(now) + '.csv']

    try:
        os.mkdir(folder + 'features')
        print("Directory created ")
        pd.Series(dummies_features).to_csv(
            names_features[0], index=False, header=False)
        pd.Series(flat_features).to_csv(
            names_features[1], index=False, header=False)
        pd.Series(time_features).to_csv(
            names_features[2], index=False, header=False)
        pd.Series(identification_features).to_csv(
            names_features[3], index=False, header=False)
    except FileExistsError:
        print("Directory already exists")

    csv = []
    for file in glob.glob(folder + "features/*.csv"):
        csv.append(file)

    try:
        table_df['current_theme_start_date'] = pd.to_datetime(
            table_df['current_theme_start_date'])
    except:
        print('REPLACE THEME START BY PSP START')
        table_df['current_theme_start_date'] = pd.to_datetime(
            table_df['current_dm_psp_start_date'])

    table_df['week_end_date'] = table_df['current_theme_start_date']

    try:
        table_df['planned_bp_flag']
    except:
        print('FILLED planned_bp_flag with 0')
        table_df['planned_bp_flag'] = 0

    try:
        table_df['active_flag']
    except:
        print('FILLED active_flag with 0')
        table_df['active_flag'] = 1

    try:
        table_df['out_stock_flag']
    except:
        try:
            table_df['out_stock_flag'] = table_df['ind_out_of_stock']
            print('FILLED out_stock_flag with ind_out_of_stock')
        except:
            print('FILLED out_stock_flag with 0')
            table_df['out_stock_flag'] = 0


    with open(f'{folder}calendar.pkl', 'rb') as input_file:
        calendar = pickle.load(input_file)

    calendar['week_end_date'] = calendar['date_value']

    table_df = table_df.merge(calendar[['week_end_date', 'week_key']], how='left')

    # ## Filter weeks with negative sales

    table_df[table_df['dm_sales_qty'] < 0].head()

    df_no_neg = table_df[(table_df['dm_sales_qty'] >= 0)
                   | (table_df['dm_sales_qty'].isna())]

    df_no_neg['dm_sales_qty'].describe()

    with open(folder + dataset_name + '.pkl', 'wb') as output_file:
        pickle.dump(
            df_no_neg, output_file)


if __name__ == '__main__':
    """[Preprocess the dataset for the dm model]
    """

    # Define variables
    folder = '97.promo_futureDMs/'
    big_table = 'forecast_sprint4_promo_mecha_v4.csv'
    sql_table = 'vartefact.forecast_sprint4_promo_mecha_v4'
    target_value = 'dm_sales_qty'
    dataset_name = 'dataset_test'

    preprocess_promo(folder=folder, big_table=big_table, sql_table=sql_table,
                     target_value=target_value, dataset_name=dataset_name)
