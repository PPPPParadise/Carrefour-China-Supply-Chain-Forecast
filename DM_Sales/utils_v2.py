# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import numpy as np
import pandas as pd
import os
import pickle
import numpy as np
import pyspark
import matplotlib.pyplot as plt
import warnings
import datetime
import csv
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
import glob
import pickle
from xgboost import plot_importance
import sys
import warnings
warnings.filterwarnings('ignore')


def _get_columns(table_name):
    """[Extract the columns names and types from the datalake]

    Arguments:
        table_name {[float]} -- [Name of the table to extract names]

    Returns:
        [list] -- [List of column names by types]
    """

    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
    os.environ["SPARK_HOME"] = '/opt/cloudera/parcels/CDH-6.1.0-1.cdh6.1.0.p0.770702/lib/spark'
    warehouse_location = abspath('spark-warehouse')

    spark = SparkSession \
        .builder \
        .appName("get_col_names") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    sql = f"""
    describe  {table_name}
    """
    sdf = spark.sql(sql)
    df1 = sdf.toPandas()

    float_list = []
    int_list = []
    timestamp = []
    string_list = []

    for index, row in df1.iterrows():
        if ('decimal' in row['data_type']) or 'double' in row['data_type']:
            float_list.append(row['col_name'])

        elif 'int' in row['data_type']:
            int_list.append(row['col_name'])

        elif 'timestamp' in row['data_type']:
            timestamp.append(row['col_name'])

        elif 'string' in row['data_type']:
            string_list.append(row['col_name'])

    if len(df1) != len(string_list) + len(timestamp) + len(int_list) + len(float_list):
        print('some columns are missing!')
        missing = df1['col_name'][
            ~df1['col_name'].isin(float_list + int_list + timestamp + string_list)]
        print('missings = ', missing)

    return int_list, float_list, timestamp, string_list


def read_data(df, table_name):
    """[Assign the proper types to a dataframe based on the Datalake]

    Arguments:
        df {[pandas.DataFrame]} -- [DataFrame to update]
        table_name {[string]} -- [Corresponding table name in the Datalake]

    Returns:
        [List] -- [Returns the dataframe and list of columns]
    """

    int_list, float_list, timestamp, string_list = _get_columns(table_name)

    float_list = float_list + ['high_temp_month',
                               'low_temp_month', 'festival_type']
    if 'high_temp_month' in string_list:
        string_list.remove('high_temp_month')
    if 'low_temp_month' in string_list:
        string_list.remove('low_temp_month')
    if 'festival_type' in string_list:
        string_list.remove('festival_type')

    df = df[np.isfinite(df['item_id'])]
    df = df[np.isfinite(df['sub_id'])]

    print(1/4)

    for i in int_list + string_list:
        try:
            df[i] = df[i].astype(int)
            try:
                string_list.remove(i)
            except:
                continue
        except:
            #print('type int, error on ', i)
            float_list.append(i)
            continue

    print(2/4)

    for i in float_list + string_list:
        try:
            df[i] = df[i].astype(float)
            try:
                string_list.remove(i)
            except:
                continue
        except:
            #print('type float, error on ', i)
            string_list.append(i)
            continue

    for i in timestamp:
        try:
            df[i] = pd.to_datetime(df[i])
        except:
            #print('type datetime, error on ', i)
            string_list.append(i)
            continue
    print(3/4)
    errors = []
    for i in string_list:
        try:
            df[i] = df[i].astype(str)
        except:
            print('type string, error on ', i)
            errors.append(i)
            continue

    print(4/4)
    df['full_item'] = df['item_id'].astype(str) + '_' + df['sub_id'].astype(str)
    df['item_store'] = df['full_item'] + '_' + df['store_code'].astype(str)

    try:
        df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    except:
        print('no column "week_end_date"')

    try:
        df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    except:
        print('no column "current_theme_start_date"')

    liste = [int_list, float_list, timestamp, string_list]

    return df, errors, liste


def read_features(features_folder):
    """[Read the features from the features folder]

    Arguments:
        features_folder {[string]} -- [Name of the folder]

    Raises:
        Exception: [If no time features is present, print warning]

    Returns:
        [list] -- [list of features]
    """
    features_files = []
    for file in glob.glob(features_folder + "/*.csv"):
        features_files.append(file)

    dumm = [s for s in features_files if 'dummies_features' in s]
    flat = [s for s in features_files if 'flat_features' in s]
    time = [s for s in features_files if 'time_features' in s]
    iden = [s for s in features_files if 'identification' in s]

    if (len(dumm) != 1) or (len(flat) != 1) or (len(time) != 1) or (len(iden) != 1):
        print('dumm', dumm)
        print('flat', flat)
        print('time', time)
        print('iden', iden)
        raise Exception('Multiple duplicated files in the features folder.')

    dumm = dumm[0]
    flat = flat[0]
    time = time[0]
    iden = iden[0]

    dummies_features = pd.read_csv(dumm, squeeze=True).tolist()
    flat_features = pd.read_csv(flat, squeeze=True).tolist()
    identification = pd.read_csv(iden, squeeze=True).tolist()

    try:
        time_features = pd.read_csv(time, squeeze=True).tolist()
        feat = dummies_features + flat_features + time_features
    except:
        time_features = []
        feat = dummies_features + flat_features
        print('Warning: no time features. Ignore if promo model.')

    return feat, dummies_features, flat_features, time_features, identification


def get_dummies(df, dummies_names):
    """[Get dummy features column names]

    Arguments:
        df {[pandas.DataFrame]} -- [Dataframe from which to get the dummies]
        dummies_names {[type]} -- [List of dummies names]

    Returns:
        [type] -- [description]
    """
    dummies_features = []
    for i in df.columns:
        if any([i.startswith(s + '_') for s in dummies_names]):
            dummies_features.append(i)

    return dummies_features


class Model():
    """[Model class to handle results of promo]

    Raises:
        Exception: [If the folder has multiple pickle files or no pickle files]
        Exception: [description]
        Exception: [description]

    Returns:
        [type] -- [Model class]
    """

    def __init__(self, folder, running_name, data_name, feature_folder='features',
                 target_value='sales_qty_sum', metric='mae', target_type='normal'):
        """[Initiate the class]

        Arguments:
            folder {[string]} -- [folder where the dataset is located]
            running_name {[string]} -- [name of the model]
            data_name {[name of the dataset]} -- [description]

        Keyword Arguments:
            feature_folder {str} -- [name of feature folder] (default: {'features'})
            target_value {str} -- [name of target value] (default: {'sales_qty_sum'})
            metric {str} -- [metric to use] (default: {'mae'})
            target_type {str} -- [depreciated: log or normal target] (default: {'normal'})

        Raises:
            Exception: [If the folder has multiple pickle files or no pickle files]
            Exception: [description]
            Exception: [description]

        Returns:
            [type] -- [description]
        """
        self.target_type = target_type
        self.target_value = target_value
        self.metric = metric
        self.running_name = running_name

        # Load model
        model_name = []
        for file in glob.glob(folder + running_name + "/*.pkl"):
            model_name.append(file)

        if len(model_name) != 1:
            print(model_name)
            raise Exception('multiple or no pikle files in the run folder!')
        else:
            print('Model name:', model_name[0])
            self.model_path = model_name[0]

        self.data_name = data_name
        self.folder = folder
        self.feature_folder = feature_folder

        self.data_path = self.folder + self.data_name

        with open(self.model_path, 'rb') as input_file:
            self.boost = pickle.load(input_file)

        with open(self.data_path, 'rb') as input_file:
            self.df = pickle.load(input_file)
            if 'next_dm_v5_extract_datetime' in self.df:
                self.df.drop(
                    columns='next_dm_v5_extract_datetime', inplace=True)

        self.df['week_end_date'] = pd.to_datetime(self.df['week_end_date'])

        self.features, self.dummies_features, self.flat_features, \
            self.time_features, self.identification = read_features(
                self.folder + self.feature_folder)

        self.now = str(datetime.datetime.now())

    def filter_dataset(self, df_to_filter):
        """[to filter a dataset]

        Arguments:
            df_to_filter {[pandas.DataFrame]} -- [dataframe to filter]

        Returns:
            [pandas.DataFrame] -- [filtered dataframe]
        """
        df = pd.DataFrame(df_to_filter.values, columns=df_to_filter.columns)

        df = df.loc[df['active_flag'] == 1]
        df = df.loc[df['planned_bp_flag'] == 0]
        df = df.loc[df['out_stock_flag'] == 0]

        out_of_stock = len(
            df_to_filter.loc[df_to_filter['out_stock_flag'] == 1])
        inactive = len(df_to_filter.loc[df_to_filter['active_flag'] == 0])
        planned_bp = len(
            df_to_filter.loc[df_to_filter['planned_bp_flag'] == 1])
        rows_filtered = out_of_stock + inactive + planned_bp
        total_row = len(df_to_filter)
        ratio = rows_filtered / total_row

        print('number of out of stocks flagged: ', out_of_stock)
        print('number of out of inactive items flagged: ', inactive)
        print('number of out of planned bp flagged: ', planned_bp)

        print('total row filtered = ', rows_filtered, ' out of ', total_row,
              '\nRatio filtered = ', ratio)

        return df

    def performance_graph(self):
        """[save performance graph]

        Returns:
            [matplotlib.pyplot] -- [figure]
        """
        results = self.boost.evals_result()
        epochs = len(results['validation_0'][self.metric])
        x_axis = range(0, epochs)
        # plot log loss
        fig, ax = plt.subplots()
        ax.plot(x_axis, results['validation_0'][self.metric], label='Train')
        ax.plot(x_axis, results['validation_1'][self.metric], label='Test')
        ax.legend()
        plt.ylabel(self.metric)
        plt.title('XGBoost ' + self.metric)
        plt.show()

        graph_name = 'performance_graph_' + self.running_name + '_' + self.now + '_.png'
        #plt.savefig(self.folder + graph_name)

        return plt

    def importance_graph(self, metric='gain'):
        """[plot importance graph]

        Keyword Arguments:
            metric {str} -- [description] (default: {'gain'})

        Returns:
            [type] -- [description]
        """
        # top 10 most important features
        ax = plot_importance(
            self.boost, max_num_features=100, importance_type=metric)
        fig = ax.figure
        fig.set_size_inches(15, 20)

        graph_name = 'importance_graph_' + self.running_name + '_' + self.now + '_.png'
        #plt.savefig(self.folder + graph_name)

        # return fig

    def save_features_importance(self, file_name):
        """[Save and return feature importance]

        Arguments:
            file_name {[str]} -- [name to save the file]

        Returns:
            [pandas.DataFrame] -- [feature importance]
        """
        features_xg = pd.DataFrame(
            self.boost.feature_importances_, columns=['value'])
        features_xg['features_names'] = self.features
        #features_xg.sort_values('value', ascending=False).to_csv(file_name, index=False)
        return features_xg

    def _weeks_to_pred(self):
        """[Returns the weeks to predict]

        Returns:
            [pandas.Series] -- [weeks to predict]
        """
        self.weeks_to_pred = self.df.loc[self.df['week_end_date'] >
                                         self.date_starting_test, 'week_key'].sort_values().unique()
        #print('weeks to pred = ', self.weeks_to_pred)
        return self.weeks_to_pred

    def predict(self, week_max=9):
        """[predict the weeks]

        Keyword Arguments:
            week_max {int} -- [description] (default: {9})
        """
        print('date starting test: ', self.date_starting_test)

        df_forecast = self.df.loc[(
            self.df['week_end_date'] >= self.date_starting_test)]
        week_starting_test = self.df.loc[(self.df['week_end_date'] >= self.date_starting_test),
                                         'week_key']
        week_starting_test = week_starting_test.min()

        df_forecast['week_key'] = df_forecast['week_key'].astype(float)

        df_forecast = df_forecast.reset_index()
        df_forecast = df_forecast.sort_values(['item_store', 'week_key'])

        df_forecast.loc[
            df_forecast['week_key'] > week_starting_test,
            self.time_features] = np.NaN

        df_forecast[self.time_features] = df_forecast[self.time_features].fillna(
            method='ffill')

        X_test = df_forecast[self.features]
        y_test = df_forecast[self.target_value]

        forecast = self.boost.predict(X_test)

        if self.target_type == 'log':
            print('We calculate the exponential of the predict!')
            df_forecast['forecast'] = np.exp(forecast)
        else:
            df_forecast['forecast'] = forecast

        usefull = ['item_store', 'week_key', 'forecast']
        weeks = self._weeks_to_pred()[:week_max].astype(float)
        df_forecast = df_forecast[df_forecast['week_key'].astype(
            float).isin(weeks)]

        self.df_forecast = df_forecast
        self.df['week_key'] = self.df['week_key'].astype(float)
        self.df = self.df.merge(df_forecast[usefull], how='left')

        print('predict sucessful!')

    def error_analysis(self, filter_type=False):
        """[add mape, rel_error and abs_error to the forecast]

        Keyword Arguments:
            filter_type {bool} -- [whether or not to filter the forecsts] (default: {False})
        """

        if filter_type == True:
            self.df_forecast = self.filter_dataset(self.df_forecast)
        else:
            print("Warning: No filter applied for the error analysis.")

        self.df_forecast['rel_error'] = (self.df_forecast['forecast'].astype(float)
                                         - self.df_forecast[self.target_value].astype(float))

        self.df_forecast['abs_error'] = abs(self.df_forecast['rel_error'])
        non_zero = self.df_forecast[self.target_value] != 0
        self.df_forecast['MAPE'] = self.df_forecast['abs_error'][non_zero] / abs(
            self.df_forecast[self.target_value][non_zero])

    def error_dataset(self, df, error_type):
        """[create a separate dataset with cumulative errors]

        Arguments:
            df {[pandas.DataFrame]} -- [dataframe original]
            error_type {[type]} -- [choose between 'rel_error', 'abs_error', and 'MAPE']

        Raises:
            Exception: [If the error type is not supported]
            Exception: [If there is no forecast in the dataset]

        Returns:
            [type] -- [description]
        """

        if error_type == 'rel_error':
            self.error = error_type
        elif error_type == 'abs_error':
            self.error = error_type
        elif error_type == 'MAPE':
            self.error = error_type
        else:
            raise Exception('Please input correct error type.')

        columns = [self.target_value, self.error, 'forecast']

        try:
            errors = df[columns].reset_index(drop=True).dropna()
        except:
            raise Exception(
                'Please use "error_analysis" function first. No error column on the DataFrame')

        errors = errors.sort_values(self.error)
        errors['cumul'] = errors[self.target_value].cumsum()
        somme = errors[self.target_value].sum()
        errors['cumul/somme'] = errors['cumul'] / somme
        errors = errors.set_index('cumul/somme')

        self.errors = errors
        return errors

    def get_MAPE(self):
        """[returns the MAPE]

        Returns:
            [type] -- [description]
        """
        return self.df_forecast['abs_error'].sum() / self.df_forecast[self.target_value].sum()

    def plot_errors(self, ylim=None):
        """[to plot errors, to use after 'error_dataset' function]

        Keyword Arguments:
            ylim {[type]} -- [y limit] (default: {None})

        Returns:
            [type] -- [figure]
        """

        fig, ax = plt.subplots()
        self.errors[self.error].plot(style='.-', figsize=(20, 10))
        ax.set_title('{} Error distribution '.format(self.error))
        ax.set_xlabel("Cumulated sales volume")
        ax.set_ylabel(self.error)
        if ylim is not None:
            ax.set_ylim(ylim)
        #fig.savefig('Sprint_2/abs_err_' + '.png')
        return fig, ax

    def error_for_volume(self, value):
        """[returns the valeur of the error for a given cumulative volume (in %)]

        Arguments:
            value {[float]} -- [value of volume]

        Returns:
            [type] -- [value of error]
        """
        print('{} for given volume:'.format(self.error))
        return self.errors[self.errors.index > value]

    def volume_for_error(self, value):
        """
        [returns the valeur of the cumulative (in %) volume for a given error]

        Arguments:
            value {[float]} -- [value of the error]

        Returns:
            [type] -- [value of error]
        """
        print('Volume for given error')
        return self.errors.loc[self.errors[self.error] > value]

    def check_forecast(self, quantile=0.999, multiplier=2):
        """[filter unlikely forecasted values]
        
        Keyword Arguments:
            quantile {float} -- [quantile value] (default: {0.999})
            multiplier {int} -- [multiplier] (default: {2})
        """
        threshold = self.df_forecast['forecast'].quantile(quantile) * multiplier
        quantile_value = self.df_forecast['forecast'].quantile(quantile)
        self.df_forecast[self.df_forecast.forecast > threshold] = quantile_value

    def predict_iter(self, nb_of_weeks=9,
                     time_features_fill_forward=['ind_competitor', 'ratio_amt_qty_new', 'disc_ratio_new']):
        """[DEPRECIATED: to predict week buy week]

        Keyword Arguments:
            nb_of_weeks {int} -- [number of week to predict] (default: {9})
            time_features_fill_forward {list} -- [time features to fill forward] (default: {['ind_competitor', 'ratio_amt_qty_new', 'disc_ratio_new']})

        Returns:
            [type] -- [description]
        """
        pass