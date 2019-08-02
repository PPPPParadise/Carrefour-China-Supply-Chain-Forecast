#!/usr/bin/env python
# coding: utf-8

import datetime
import os
import pickle
import sys
import warnings
from importlib import reload

import numpy as np
import pandas as pd
import utils_v2
import xgboost as xgb
from sklearn.model_selection import KFold
from utils_v2 import Model
from utils_v2 import read_features

warnings.filterwarnings('ignore')
pd.set_option('mode.use_inf_as_na', True)

sys.path.append(os.getcwd())
reload(utils_v2)


def train(desc, folder, data_name, target_value, learning_rate, date_stop_train):
    """Train a XGBoost model to predict the sales, 
    and train a XGBoost model to predict the standard deviation of the error


    Arguments:
        desc {[string]} -- [Name of the model training]
        folder {[string]} -- [Folder containing the dataset]
        data_name {[string]} -- [name of the dataset]
        target_value {[string]} -- [name of the column to predict]
        learning_rate {[float]} -- [learning rate of the model]
        date_stop_train {[type]} -- [Date to stop the training, format YYYY-MM-DD]

    Returns:
        ['string'] -- [Folder where the model is saved]
    """

    # Number of splits for KFold
    n_splits = 3

    running_name = desc
    dataset_name = folder + data_name

    print('Dataset used : ', dataset_name)

    #### Load and preprocess
    features_folder = folder + 'features'

    features, dummy_features, flat_features, \
    time_features, identification_features = read_features(features_folder)

    with open(dataset_name, 'rb') as input_file:
        df = pickle.load(input_file)

    # Train/Test/CV selection
    date_end_training = datetime.datetime.strptime(date_stop_train, '%Y-%m-%d')
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])

    # Filter for the training
    df_filtered = df.copy()

    # Train only on 2018 and filter negative sales
    df_filtered = df_filtered[df_filtered['week_end_date']
                              >= datetime.date(2018, 1, 1)]
    df_filtered = df_filtered.loc[df_filtered[target_value] > 0]
    df = df_filtered

    train = df.loc[df['week_end_date'] < date_end_training]
    test = df.loc[(df['week_end_date'] >= date_end_training)]

    #### X_train and y_train
    X_train = train[features]
    y_train = train[target_value]

    print('train of length ', len(train))
    print('test of length', len(test))

    # Model training

    hyp = {
        'silent': False,
        'learning_rate': learning_rate,
        'n_estimators': 1000,
        'max_depth': 6,
        'sub_sample': 0.7,
        'gamma': 5,
        'colsample_bytree': 0.8,
        'n_jobs': 15,

    }

    hyp_error = {
        'silent': False,
        'learning_rate': learning_rate,
        'n_estimators': 100,
        'max_depth': 6,
        'sub_sample': 0.7,
        'gamma': 5,
        'colsample_bytree': 0.8,
        'n_jobs': 15,

    }

    sales_prediction_model = xgb.XGBRegressor(**hyp)
    sales_prediction_squared_error_model = xgb.XGBRegressor(**hyp_error)

    # Kfold loop
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=7)
    score = 0.0
    best_scores = []
    results = np.zeros(len(X_train))

    for train_index, test_index in kf.split(X_train):
        # Train forecast model
        X_tr, X_te = X_train.iloc[train_index], X_train.iloc[test_index]
        y_tr, y_te = y_train.iloc[train_index], y_train.iloc[test_index]
        eval_set = [(X_tr, y_tr), (X_te, y_te)]

        sales_prediction_model.fit(X_tr, y_tr, eval_set=eval_set, verbose=True,
                                   early_stopping_rounds=20,
                                   eval_metric='mae'
                                   )

        # Show and save performance on the training
        best_scores.append(sales_prediction_model.best_score)
        mape = abs(results[test_index] - y_te.values).sum() / y_te.sum()
        print('Mape of:', mape)
        score += mape

        # Train the error squared predictor
        results[test_index] = sales_prediction_model.predict(X_te)
        error_y_test = (results[test_index] - y_te) ** 2
        sales_prediction_squared_error_model.fit(X_te, error_y_test, verbose=False,
                                                 eval_metric="mae")

    score /= n_splits
    print('Training successful!!')

    # Saving results
    now = datetime.datetime.now().strftime("%m-%d-%H-%M-%S")
    saving_folder = folder + running_name + '_created_on_' + now
    folder_name = running_name + '_created_on_' + now

    try:
        os.mkdir(saving_folder)
        print("Directory ", saving_folder, " Created ")
    except FileExistsError:
        print("Directory ", saving_folder, " already exists")

    # Save sales forecast model
    sales_prediction_model.save_model(saving_folder + '/model_' + running_name +
                                      '_' + str(now) + '_.model'
                                      )

    with open(saving_folder + '/model_' + running_name +
              '_' + str(now) + '_.pkl', 'wb') as output_file:
        pickle.dump(sales_prediction_model, output_file)

    # Save error squared predictor model
    with open(saving_folder + '/model_error.pkl2', 'wb') as output_file:
        pickle.dump(sales_prediction_squared_error_model, output_file)

    # Save hyperparameters
    pd.DataFrame(hyp.values(), index=hyp.keys()).to_csv(
        saving_folder + '/hyperparameters_' + running_name + '_' + str(now) + '.csv')

    print('Model and hyperparameters saved.')
    return folder_name


def predict(desc, folder, data_name, target_value, learning_rate, date_stop_train):
    """[Create a prediction]

    Arguments:
        desc {[string]} -- [Name of the model training]
        folder {[string]} -- [Folder containing the dataset]
        data_name {[string]} -- [name of the dataset]
        target_value {[string]} -- [name of the column to predict]
        learning_rate {[float]} -- [learning rate of the model]
        date_stop_train {[type]} -- [Date to stop the training, format YYYY-MM-DD]
    """

    running_name = desc
    feature_folder = 'features'

    model_class = Model(folder, running_name, data_name, feature_folder, metric="mae",
                        target_type='normal', target_value='dm_sales_qty')

    date_end_training = datetime.datetime.strptime(date_stop_train, '%Y-%m-%d')
    model_class.date_starting_test = date_end_training

    # Predict
    model_class.predict(70)

    # Filter negative forecasts and remove unlikely forecasts
    model_class.df_forecast['forecast'][model_class.df_forecast['forecast'] < 0] = 0
    model_class.check_forecast()

    try:
        os.mkdir(folder + running_name + '/analyses')
        print("Directory created ")
    except FileExistsError:
        print("Directory already exists")

    # Save feature importance
    model_class.save_features_importance(
        folder + running_name + '/analyses/' + 'features_importance.csv')

    try:
        os.mkdir(folder + running_name + '/predictions')
        print("Directory created ")
    except FileExistsError:
        print("Directory already exists")

    # Save sales predictions
    pickle.dump(model_class.df_forecast, open(folder + running_name + '/predictions' +
                                              '/forecast_dump.pkl', 'wb'))

    # Load the error squared predictor model
    with open(folder + running_name + '/model_error.pkl2', 'rb') as input_file:
        sales_prediction_squared_error_model = pickle.load(input_file)

    features_predictions_df = model_class.df_forecast
    model_features = model_class.features

    # Predict the error
    features_predictions_df.loc[:, 'squared_error_predicted'] = abs(
        sales_prediction_squared_error_model.predict(features_predictions_df[model_features]))

    # Calculate std
    features_predictions_df.loc[:,
    'std_dev_predicted'] = features_predictions_df.squared_error_predicted ** 0.5

    # Evaluate 80% confidence interval
    features_predictions_df.loc[:, 'confidence_interval_80_max'] = (
            features_predictions_df['forecast'] + 1.28 * features_predictions_df.std_dev_predicted)

    features_predictions_df.loc[:,
    'confidence_interval_max'] = features_predictions_df.confidence_interval_80_max
    features_predictions_df.loc[:,
    'order_prediction'] = features_predictions_df.confidence_interval_80_max

    features_predictions_df['sales'] = features_predictions_df['dm_sales_qty']
    features_predictions_df['sales_prediction'] = features_predictions_df['forecast']

    # Save the predictions results with confidence interval
    final_results_item_store_dm = features_predictions_df[[
        'item_store',
        'item_id',
        'sub_id',
        'sub_family_code',
        'store_code',
        'current_dm_theme_id',
        'sales',
        'sales_prediction',
        'squared_error_predicted',
        'std_dev_predicted',
        'confidence_interval_max',
        'order_prediction'
    ]]

    final_results_item_store_dm \
        .to_csv(folder + running_name + '/promo_sales_order_prediction_by_item_store_dm.csv', index=False)


if __name__ == '__main__':
    """[Train and save predictions for promo model]
    """

    desc = 'promo_97_train_until_01-07-2019'
    folder = '97.promo_futureDMs/'
    data_name = 'dataset_promo.pkl'
    target_value = 'dm_sales_qty'
    date_stop_train = '2019-07-01'
    learning_rate = 0.3

    folder_name = train(desc=desc, folder=folder, data_name=data_name, target_value=target_value,
                        learning_rate=learning_rate, date_stop_train=date_stop_train)

    predict(folder_name, folder, data_name,
            target_value, learning_rate, date_stop_train)
