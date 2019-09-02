from download_data import *
from weekly_model_preprocessing import *
from weekly_model_train import *
import datetime
import os
proc_root = os.path.dirname(os.path.realpath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lpad,split
from impala.dbapi import connect
import argparse
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
warehouse_location = abspath('spark-warehouse')

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database_name", help="database name")
parser.add_argument("-f", "--local_folder", help="local folder")
parser.add_argument("-s", "--date_stop_train", help="date stop train")
parser.add_argument("-c", "--config_folder", help="config folder")
args = parser.parse_args()

# config = {}
# config['database'] = 'temp'
# config['local_folder'] = 'test_3_folder_weekly/'
# config['date_stop_train'] = '2019-07-01'
# python3 /data/jupyter/ws_vincent/Forecast3/roger_handover/all_included_weekly.py -d temp -f '/data/jupyter/ws_vincent/Forecast3/roger_handover/test_3_folder_weekly/' -s '2019-07-01'
config = {}
config['database'] = args.database_name
config['local_folder'] = args.local_folder
config['date_stop_train'] = args.date_stop_train
config['config_folder'] = args.config_folder
print(config)
if (args.database_name is None) or (args.local_folder is None) or (args.date_stop_train is None):
    print('config needed ')
    sys.exit()
    
def impalaexec(sql,create_table=False):
    """
    execute sql using impala
    """
    print(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database']) as conn:
        curr = conn.cursor()
        curr.execute(sql)

def download_csv_by_spark(spark,sql_query,file_name):
    os.system(f'hadoop fs -rm -r {file_name}')
    os.system(f'hadoop fs -rm -r {file_name}.csv')
    df_trxn_all = spark.sql(sql_query)
    df_trxn_all.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv').option("header", "true").save(file_name)
    os.system(f'hadoop fs -ls {file_name} > out_put.txt')
    for r in open('out_put.txt'):
        result = r
    file_name_on_hdfs = result.split(' ')[-1].strip()
    os.system(f'hadoop fs -text {file_name_on_hdfs} | hadoop fs -put - {file_name}.csv   ')
    os.system(f"hadoop fs -get -f {file_name}.csv {config['local_folder']}")
    print('download success')
    return pd.read_csv(f"{config['local_folder']}{file_name}.csv")

def main():
    # Download data
    now = datetime.datetime.now().strftime("%m-%d-%H-%M-%S")
    folder = config['local_folder']
    os.system(f"rm -r {folder}")
    
    
    # Daily dataset
    ##big_table_name = 'vartefact.forecast_sprint4_add_dm_to_daily'

    # Promo dataset
    ##big_table_name = 'vartefact.forecast_sprint4_promo_mecha_v4'

    # Weekly dataset
    big_table_name = f"{config['database']}.forecast_sprint3_v10_flag_sprint4"
    file_name = 'test_to_delete_' + now
    download_data(folder=folder, big_table_name=big_table_name, file_name=file_name)

    # Preprocess the datase
    big_table = file_name + '.csv'
    sql_table = big_table_name
    target_value = 'sales_qty_sum'
    dataset_name = 'dataset_test_weekly'
    preprocess(folder=folder, big_table=big_table, sql_table=sql_table,
                     target_value=target_value, dataset_name=dataset_name)

    # Train the model
    desc = 'weekly_test'
    data_set1 = dataset_name + '_part1.pkl'
    data_set2 = dataset_name + '_part2.pkl'
    
    date_stop_train = config['date_stop_train']
    os.system(f"cp {config['config_folder']}/calendar.pkl {config['local_folder']}")
    # learning_rate = 0.3

    run_model(folder=folder, data_set1=data_set1, data_set2=data_set2, futur_prediction=True,
                        date_stop_train=date_stop_train)
    # save csv as table 
    print('saving csv as table')
    file_list = os.listdir(config['local_folder'])

    for f in file_list:
        if 'weekly_model_training_for_futur_predictions' in f:
            result_file_name = f
    result_csv_path = f"{config['local_folder']}/{result_file_name}/resulst_forecast_10w_on_the_fututre.csv"
    os.system(f"hadoop fs -rm resulst_forecast_10w_on_the_fututre.csv")
    os.system(f"hadoop fs -put -f {result_csv_path}")

    spark = SparkSession \
        .builder \
        .appName("Forecast_saveastable") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.num.executors", '10') \
        .config("spark.executor.memory", '15G') \
        .config("spark.executor.cores", '20') \
        .enableHiveSupport() \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(f"resulst_forecast_10w_on_the_fututre.csv")
    split_col = pyspark.sql.functions.split(spark_df['full_item'], '_')
    spark_df = spark_df.withColumn('item_id', split_col.getItem(0))
    spark_df = spark_df.withColumn('sub_id', split_col.getItem(1))
    spark_df = spark_df.withColumn("item_id", spark_df["item_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("sub_id", spark_df["sub_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("week", spark_df["week"].cast(IntegerType()))
    spark_df = spark_df.withColumn("train_mape_score", spark_df["train_mape_score"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales", spark_df["predict_sales"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales_error_squared", spark_df["predict_sales_error_squared"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales_max_confidence_interval", spark_df["predict_sales_max_confidence_interval"].cast(FloatType()))
    spark_df = spark_df.withColumn("order_prediction", spark_df["order_prediction"].cast(FloatType()))
    spark_df = spark_df.withColumn("store_code", lpad(spark_df['store_code'],3,'0'))
    spark_df = spark_df.withColumnRenamed('week','week_key')
    spark_df = spark_df.withColumnRenamed('predict_sales','sales_prediction')
    spark_df = spark_df.withColumnRenamed('predict_sales_max_confidence_interval','max_confidence_interval')
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database']}.result_forecast_10w_on_the_fututre")
    sql = f""" invalidate metadata {config['database']}.result_forecast_10w_on_the_fututre """
    impalaexec(sql)
    print('csv saved in the table')

    ############ forecast_sprint3_v10_flag_sprint4 sales information
    ## get max_week_key min_week_key
    big_table_name = f"{config['database']}.forecast_sprint3_v10_flag_sprint4"
    week_numbes_to_roll_median = 4
    sql_query = f"""
    select 
        cast((cast(max(week_key) as int) - {week_numbes_to_roll_median})  as varchar(10)) as min_week_key,
        max(week_key) as max_week_key
    from {big_table_name}
    """
    week_keys = spark.sql(sql_query).toPandas()
    max_week = week_keys['max_week_key'].iloc[0]
    min_week =  week_keys['min_week_key'].iloc[0]

    ## get median median_sales
    sql_query = f"""
    select sales_qty_sum, sub_id, week_key, store_code
    from {big_table_name}
    where week_key >= {min_week} 
    """
    df_from_sql = spark.sql(sql_query).toPandas()
    df_from_sql['sales_qty_sum'] = df_from_sql['sales_qty_sum'].astype(float)
    median_df = df_from_sql[['sales_qty_sum', 'sub_id', 'store_code']].groupby(['sub_id', 'store_code']).median().reset_index()
    median_df = median_df.rename({'sales_qty_sum': 'median_sales'}, axis=1)
    ############ result_forecast_10w_on_the_fututre_all prediction result information
    results_table = f"{config['database']}.result_forecast_10w_on_the_fututre"
    sql_query = f"""
    select *
    from {results_table}
    """
    file_name = 'results'
    forecast_df = download_csv_by_spark(spark,sql_query,file_name)
    print('length before merge: ', len(forecast_df))
    key_columns = ['sub_id', 'store_code']
    for col in key_columns:
        median_df[col] = median_df[col].astype(float)
        forecast_df[col] = forecast_df[col].astype(float)
    forecast_df_w_sales = forecast_df.merge(median_df, how='left')
    print('after merge: ', len(forecast_df_w_sales))
    forecast_df_w_sales.loc[:, 'forecast_min_median'] = forecast_df_w_sales[['median_sales', 'order_prediction']].max(axis=1)
    forecast_df_w_sales.loc[(forecast_df_w_sales['sales_prediction'] > 7)
                            & (forecast_df_w_sales['forecast_min_median'] > forecast_df_w_sales['median_sales']), 
                            'max_order_3xmedian'] = np.minimum(3 * forecast_df_w_sales['median_sales'], forecast_df_w_sales['forecast_min_median'])
    df_prediction_final = forecast_df_w_sales.copy()
    df_prediction_final['max_confidence_interval'] = df_prediction_final[['max_order_3xmedian', 'forecast_min_median']].min(axis=1)
    df_prediction_final['sales_prediction'] = df_prediction_final[['max_confidence_interval', 'sales_prediction']].min(axis=1)
    df_prediction_final['order_prediction'] = df_prediction_final[['order_prediction', 'max_confidence_interval']].min(axis=1)

    df_prediction_final= df_prediction_final.drop(
        ['median_sales', 'forecast_min_median', 'max_order_3xmedian'], axis=1)
    df_prediction_final.describe().T
    df_prediction_final.to_csv(config['local_folder']+'df_prediction_final_roger_normal_adhoc.csv', index=False)
    result_csv_path = f"{config['local_folder']}df_prediction_final_roger_normal_adhoc.csv"
    os.system(f"hadoop fs -rm df_prediction_final_roger_normal_adhoc.csv")
    os.system(f"hadoop fs -put -f {result_csv_path}")
    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(f"df_prediction_final_roger_normal_adhoc.csv")
    split_col = split(spark_df['full_item'], '_')
    spark_df = spark_df.withColumn('item_id', split_col.getItem(0))
    spark_df = spark_df.withColumn('sub_id', split_col.getItem(1))
    spark_df = spark_df.withColumn("item_id", spark_df["item_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("sub_id", spark_df["sub_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("week_key", spark_df["week_key"].cast(IntegerType()))
    spark_df = spark_df.withColumn("train_mape_score", spark_df["train_mape_score"].cast(FloatType()))
    spark_df = spark_df.withColumn("sales_prediction", spark_df["sales_prediction"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales_error_squared", spark_df["predict_sales_error_squared"].cast(FloatType()))
    spark_df = spark_df.withColumn("max_confidence_interval", spark_df["max_confidence_interval"].cast(FloatType()))
    spark_df = spark_df.withColumn("order_prediction", spark_df["order_prediction"].cast(FloatType()))
    spark_df = spark_df.withColumn("store_code", lpad(spark_df['store_code'],3,'0'))
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database']}.result_forecast_10w_on_the_fututre")
    spark.stop()

    sql = f""" invalidate metadata {config['database']}.result_forecast_10w_on_the_fututre """
    impalaexec(sql)
    print('csv saved in the table')
    


if __name__ == '__main__':
    main()
