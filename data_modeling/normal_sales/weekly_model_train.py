# # Rolling week predict simple version. Train once and rolling predict

# # Import

import datetime
import itertools
import os
import pickle
import shutil
import sys
from datetime import timedelta
from importlib import reload

import numpy as np
import pandas as pd
import utils_v2
import xgboost as xgb
from sklearn.model_selection import KFold
from utils_v2 import read_features

sys.path.append(os.getcwd())
reload(utils_v2)


# # Load input from file


def run_model(folder, data_set1, data_set2, future_prediction, date_stop_train):
    """[To train and predict using the weekly model]

    Arguments:
        folder {[string]} -- [Name of the folder containing data]
        data_set1 {[string]} -- [Name of the 1st part of the dataset (pickle file)]
        data_set2 {[string]} -- [Name of the 2nd part of the dataset (pickle file)]
        future_prediction {[bool]} -- [If the prediction is on futur unknown values,
                                        we delete some useless columns from the result file,
                                        ]
        date_stop_train {[string]} -- [Date to stop the training, format YYYY-MM-DD]
    """

    calendar_file = folder + 'calendar.pkl' # Review: check dependency from Carrefour's file
    date_stop_train = datetime.datetime.strptime(date_stop_train, '%Y-%m-%d')

    print(datetime.datetime.now(), "Start")
    print(datetime.datetime.now(), "Load data")

    with open(folder + data_set1, 'rb') as input_file:
        input_df_1 = pickle.load(input_file)

    with open(folder + data_set2, 'rb') as input_file:
        input_df_2 = pickle.load(input_file)

    input_df = pd.concat([input_df_1, input_df_2]).reset_index(drop=True)

    with open(calendar_file, 'rb') as input_file:
        calendar_df = pickle.load(input_file)

    feat, dummies_features, flat_features, time_features, identification = read_features(
        folder + 'features')

    desc_df = input_df[['sales_qty_sum', 'full_item']].groupby(
        'full_item').agg(['describe', sum])
    desc_df.columns = ['count', 'mean', 'std',
                      'min', '25%', '50%', '75%', 'max', 'sum']
    desc_df = desc_df.sort_values(by='sum', ascending=False)
    desc_df['cum_sum'] = desc_df['sum'].cumsum()
    desc_df['sale_cum_w'] = desc_df['sum'].cumsum() / input_df.sales_qty_sum.sum()
    desc_df.head()

    item_list = list(desc_df.index)

    # # Model variables

    # input date should be a Sunday.
    # Week end with this Sunday is the last week in traning
    # Week starting from next day (the Monday) is week 1

    # To update if prediction on multiple starting date is needed
    # Not implemented so far
    number_of_weeks = 1

    # To predict every week from 1 to 10 included
    predict_week = [i for i in range(1, 11)]

    # output folder
    model_name = "forecast_10w_on_the_fututre"

    top10_feature_file = f"top10_feature_{model_name}.csv"
    top50_all_file = f"resulst_{model_name}.csv"
    skipped_week_file = f"skipped_week_{model_name}.csv"

    if future_prediction:
        save_path = folder + "weekly_model_training_for_futur_predictions_created_on_" + \
                   datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    else:
        save_path = folder + "weekly_model_training_for_result_evaluation_created_on_" + \
                   datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    # week_shift = predict_week - 1
    week_shift = [x - 1 for x in predict_week]

    target_week_value = ['w{}_sales_qty'.format(x) for x in week_shift]
    target_week_value_copied = target_week_value

    # cut_item_list = item_list[0:10]

    input_df["week_end_date"] = pd.to_datetime(
        input_df["week_end_date"], format="%Y-%m-%d")

    # # Model Logic

    # +
    print(datetime.datetime.now(), 'Start model processing')

    try:
        shutil.rmtree(f'{save_path}/model_result')
    except:
        pass

    try:
        os.makedirs(f'{save_path}/model_result')
    except:
        pass

    print(datetime.datetime.now(), " start")

    # Save the feature importance
    pd.DataFrame(["feature_1", "feature_2", "feature_3",
                  "feature_4", "feature_5", "feature_6",
                  "feature_7", "feature_8", "feature_9",
                  "feature_10", "item_id", "week", "index"]).transpose() \
        .to_csv(f'{save_path}/{top10_feature_file}', mode='w', index=False, header=False)

    # Save the items and weeks that have been skipped
    pd.DataFrame(['item_store', 'skipped_week', 'sales_qty_sum', 'w{}_sales_qty'
                 .format(week_shift)]).transpose() \
        .to_csv(f'{save_path}/{skipped_week_file}', mode='w', index=False, header=False)

    # Save the log files
    log_file = open(
        f'{save_path}/{model_name}_log_{str(datetime.datetime.now())}.txt', "a")
    error_file = open(
        f'{save_path}/{model_name}_error_{str(datetime.datetime.now())}.txt', "a")

    counter = 0
    progress = int(len(item_list) / 100) + 1
    progress_counter = 1

    # for item
    for item in item_list:

        counter = counter + 1

        if (counter % progress) == 0:
            print(datetime.datetime.now(),
                  f' item {counter}, {progress_counter}% finished')
            progress_counter = progress_counter + 1

        log_file.write(str(datetime.datetime.now()) +
                      ' start item ' + str(counter) + "\n")

        # If we predict on the future, we can't compare the results to the reality

        if future_prediction:
            score_dict = {'full_item': [], 'store_code': [],
                          'week': [], 'train_mape_score': [],
                          'predict_sales': [],
                          'predict_sales_error_squared': [],
                          'predict_sales_max_confidence_interval': [],
                          'order_prediction': []}

        else:
            score_dict = {'full_item': [], 'store_code': [],
                          'week': [], 'train_mape_score': [],
                          'predict_mape_score': [],
                          'cumul/somme': [],
                          'rel_error': [],
                          'actual_sales': [],
                          'predict_sales': [],
                          'predict_sales_error_squared': [],
                          'predict_sales_max_confidence_interval': [],
                          'order_prediction': []}

        # get data for item only
        df_one_item = input_df[(input_df['full_item'] == item)]

        # Get all week keys within training data time period.
        week_key_min = df_one_item['week_key'].min()
        week_key_max = df_one_item['week_key'].max()
        week_keys_df = calendar_df.loc[(calendar_df['week_key'] >= week_key_min)
                                     & (calendar_df['week_key'] <= week_key_max), ['week_key', 'week_end_date']] \
            .drop_duplicates()

        # Get all stores
        stores_df = df_one_item["item_store"].drop_duplicates()

        # Generate all store and week key combinations
        all_week_stores_df = pd.DataFrame(itertools.product(
            stores_df, week_keys_df["week_key"].transpose()))
        all_week_stores_df.columns = ['item_store', 'week_key']

        # Outer join the training data to the combinations.
        # The combination with no transaction will be handled
        df_one_item_w = pd.merge(all_week_stores_df, df_one_item, on=[
            'week_key', 'item_store'], how='outer')

        # Remove all the weeks with no weekly sales or no to-predict weekly sales (e.g. week 2 sales)
        # The method is fill it with not possible value and filter by this value
        df_one_item_w.fillna({'sales_qty_sum': -1}, inplace=True)

        # Create 10 colums that corresponds to the target value to predict
        # Sales of week1, sales of week2, etc..
        # Flag the weeks with missing sales quantity by assigning -1 to the value
        for week_shift_number in week_shift:
            df_one_item_w['w{}_sales_qty'.format(week_shift_number)] = \
                df_one_item_w[['sales_qty_sum', 'week_key', 'item_store']].groupby(
                    ['item_store']).shift(-week_shift_number).reset_index()['sales_qty_sum'].values

            df_one_item_w[
                (df_one_item_w['sales_qty_sum'] == -1) | (df_one_item_w['w{}_sales_qty'.format(week_shift_number)] == -1)][[
                'item_store', 'week_key', 'sales_qty_sum', 'w{}_sales_qty'.format(week_shift_number)]] \
                .to_csv(f'{save_path}/{skipped_week_file}', mode='a', index=False, header=False)

            df_one_final = df_one_item_w[
                (df_one_item_w['sales_qty_sum'] != -1) & (df_one_item_w['w{}_sales_qty'.format(week_shift_number)] != -1)]

        # All features
        features = flat_features + time_features

        week_end = date_stop_train

        # Train/Test split
        train = df_one_final.loc[df_one_final['week_end_date'] < date_stop_train]
        test = df_one_final.loc[df_one_final['week_end_date'] >= date_stop_train]

        # Weekly loops : we train one model per week

        for target_week_value, week in zip(target_week_value_copied, week_shift):

            train = train[np.isfinite(train[target_week_value])]

            X_train = train[features]
            y_train = train[target_week_value]

            y_test = test[target_week_value]

            score = 0.0

            try:
                results = np.zeros(len(X_train))
                sales_prediction_model = xgb.XGBRegressor(
                    silent=False,
                    learning_rate=0.03,
                    n_estimators=10000,
                    max_depth=4,
                    gamma=1,
                    colsample_bytree=0.8,
                    n_jobs=30
                )

                sales_prediction_squared_error_model = xgb.XGBRegressor(
                    silent=False,
                    learning_rate=0.03,
                    n_estimators=100,
                    max_depth=4,
                    gamma=1,
                    colsample_bytree=0.8,
                    n_jobs=30
                )

                num_folds = 2

                # The item needs to have at least 2 rows of data
                if len(X_train) > num_folds:
                    pass
                else:
                    error_file.write("".join([str(datetime.datetime.now()), ', index ', str(counter),
                                             ', item ', item, ', week ', str(
                            week),
                                             ', target value, ', target_week_value, ' does not have enough points\n']))
                    continue

                kf = KFold(n_splits=num_folds, shuffle=False, random_state=7)

                # REVIEW: do we need kfold for training and analysing ?
                # KFold training
                for train_index, test_index in kf.split(X_train):
                    X_tr, X_te = X_train.iloc[train_index], X_train.iloc[test_index]
                    y_tr, y_te = y_train.iloc[train_index], y_train.iloc[test_index]
                    eval_set = [(X_tr, y_tr), (X_te, y_te)]
                    sales_prediction_model.fit(X_tr, y_tr, verbose=False,
                                               early_stopping_rounds=15,
                                               eval_set=eval_set, eval_metric="mae")
                    results[test_index] = sales_prediction_model.predict(X_te)

                    mape = abs(results[test_index] -
                               y_te.values).sum() / y_te.sum()
                    score += mape

                    # Train the error squared predictor
                    error_y_test = (results[test_index] - y_te) ** 2

                    sales_prediction_squared_error_model.fit(X_te, error_y_test, verbose=False,
                                                             eval_metric="mae")

                score /= num_folds

            except Exception as e:
                # print('error for target_value:', target_week_value, 'and week', week)

                error_file.write("".join([str(datetime.datetime.now()),
                                         ', index ', str(
                        counter), ', item ', item,
                                         ', week ', str(week), ', ', str(e), "\n"]))

            else:
                # Save the top 10 features of the model
                features_xg = pd.DataFrame(
                    sales_prediction_model.feature_importances_, columns=['feature_value'])
                features_xg.index = features
                top10Feature = features_xg.sort_values(
                    'feature_value', ascending=False).head(10)
                top10Feature["features"] = top10Feature.index + \
                                           ":" + top10Feature["feature_value"].astype(str)
                top10Feature = top10Feature[["features"]]
                top10Feature = top10Feature.transpose()
                top10Feature["item_id"] = item
                top10Feature["week"] = 'week' + str(week)
                top10Feature["index"] = str(counter)
                top10Feature.to_csv(
                    f'{save_path}/{top10_feature_file}', mode='a', index=False, header=False)

                filename = f'{save_path}/model_result/{item}!{week}.model'

                pickle.dump(sales_prediction_model, open(filename, 'wb'))

                # Create a prediction using the trained model
                for i in range(0, number_of_weeks):

                    # The week sales to predict
                    predict_week_end = date_stop_train + timedelta(days=7 * i)
                    predict_week_key = calendar_df[calendar_df['date_value']
                                                  == predict_week_end]["week_key"].min()

                    # The input data to perform perdict
                    test = df_one_final.loc[df_one_final['week_key']
                                           == predict_week_key]
                    test = test.sort_values(
                        ['item_store', 'week_key'], ascending=True)

                    X_test = test[features]
                    if future_prediction:
                        pass
                    else:
                        y_test = test[target_week_value]

                    forecast = sales_prediction_model.predict(X_test)
                    error_squared_forecast = sales_prediction_squared_error_model.predict(
                        X_test)

                    test.loc[:, 'forecast'] = forecast
                    test.loc[:, 'error_squared_forecast'] = error_squared_forecast

                    # Change the value here to modify the desired confidence interval
                    # As reference: 3* = 90% interval, 1.28* = 80% interval... Cf normal distribution

                    test.loc[:, 'predict_sales_max_confidence_interval'] = (
                            test.forecast + 3 * (test.error_squared_forecast ** 0.5))

                    # We compute the week's order by subtracting the overstock of
                    # the last week (max CI - estimated sales) from this week's max CI
                    # (CI : confidence interval)

                    test.loc[:, 'order_prediction'] = (
                            test.predict_sales_max_confidence_interval -
                            (test[['item_store',
                                   'week_key',
                                   'predict_sales_max_confidence_interval']]
                             .groupby(['item_store'])
                             .shift(1)
                             .predict_sales_max_confidence_interval
                             - test[['item_store',
                                     'week_key',
                                     'forecast']]
                             .groupby(['item_store'])
                             .shift(1)
                             .forecast))

                    test.order_prediction = test.order_prediction.fillna(
                        test.predict_sales_max_confidence_interval)

                    order_prediction = list(test.order_prediction)

                    # If the predictions are not on the futur, we can compute the metrics

                    if future_prediction:
                        pass
                    else:
                        errors = pd.DataFrame(
                            (forecast - y_test).values, columns=['rel_error'])
                        errors['sales_qty_sum'] = y_test.values
                        errors = errors.sort_values('rel_error')
                        errors['cumul'] = errors['sales_qty_sum'].cumsum()
                        somme = errors['sales_qty_sum'].sum()
                        errors['cumul/somme'] = errors['cumul'] / somme
                        errors = errors.set_index('cumul/somme')
                        rel_error_each_list = list((forecast - y_test).values)
                        actual_sales = list(y_test)

                    rel_error_store_list = list(test['store_code'])
                    predict_sales = list(test.forecast)
                    predict_sales_error_squared = list(
                        test.error_squared_forecast)
                    predict_sales_max_confidence_interval = list(
                        test.predict_sales_max_confidence_interval)

                    # Save the metrics in a dictionary
                    for i in range(0, len(forecast)):
                        score_dict['full_item'].append(item)
                        score_dict['week'].append(predict_week_key + week)
                        score_dict['train_mape_score'].append(score)

                        if future_prediction:
                            pass
                        else:
                            score_dict['predict_mape_score'].append(
                                abs(forecast - y_test).sum() / y_test.sum())
                            try:
                                score_dict['cumul/somme'].append(
                                    errors[errors['rel_error'] >= 0].index[0])
                            except Exception as e:
                                score_dict['cumul/somme'].append(0)
                            score_dict['rel_error'].append(
                                rel_error_each_list[i])
                            score_dict['actual_sales'].append(actual_sales[i])

                        score_dict['store_code'].append(
                            rel_error_store_list[i])
                        score_dict['predict_sales'].append(predict_sales[i])
                        score_dict['predict_sales_error_squared'] \
                            .append(predict_sales_error_squared[i])
                        score_dict['predict_sales_max_confidence_interval'] \
                            .append(predict_sales_max_confidence_interval[i])
                        score_dict['order_prediction'] \
                            .append(order_prediction[i])

        # Save the metrics in a csv

        score_df = pd.DataFrame(score_dict)
        if counter == 1:
            score_df[score_df.full_item == 'head_only'].to_csv(
                f'{save_path}/{top50_all_file}', mode='w', index=False)

        score_df.to_csv(f'{save_path}/{top50_all_file}',
                        mode='a', index=False, header=False)
        log_file.write(str(datetime.datetime.now()) +
                      ' end item index ' + str(counter) + "\n")

    log_file.write(str(datetime.datetime.now()) + " finish\n")
    print(datetime.datetime.now(), '100% finished\n')

    log_file.close()
    error_file.close()
    # -


if __name__ == '__main__':
    """[To train and predict the weekly model]
    """

    folder = '83.new_bp/'
    data_set1 = 'dataset_1307dataset_1307_part1.pkl'
    data_set2 = 'dataset_1307dataset_1307_part2.pkl'
    date_stop_train = '2019-01-13'
    future_prediction = False

    run_model(folder, data_set1, data_set2, future_prediction, date_stop_train)
