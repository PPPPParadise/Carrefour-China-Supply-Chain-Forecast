import datetime

from download_data import download_data
from promo_model_preprocessing import preprocess_promo
from promo_model_train_with_confidence import train, predict

proc_root = os.path.dirname(os.path.realpath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import lpad
from impala.dbapi import connect
import argparse

os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
warehouse_location = abspath('spark-warehouse')

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database_name", help="database name")
parser.add_argument("-f", "--local_folder", help="local folder")
parser.add_argument("-s", "--date_stop_train", help="date stop train")
args = parser.parse_args()

config = {}
config['database'] = args.database_name
config['local_folder'] = args.local_folder
config['date_stop_train'] = args.date_stop_train
print(config)
if (args.database_name is None) or (args.local_folder is None) or (args.date_stop_train is None):
    print('config needed ')
    sys.exit()


def impalaexec(sql, create_table=False):
    """
    execute sql using impala
    """
    print(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019',
                 database=config['database']) as conn:
        curr = conn.cursor()
        curr.execute(sql)


def main():
    # Download data
    now = datetime.datetime.now().strftime("%m-%d-%H-%M-%S")
    folder = config['local_folder']
    os.system(f"rm -r {folder}")

    # Promo dataset
    big_table_name = f"{config['database']}.forecast_sprint4_promo_mecha_v4"
    file_name = 'test_to_delete_' + now
    download_data(folder=folder, big_table_name=big_table_name, file_name=file_name)

    os.system(f"cp {proc_root}/calendar.pkl {config['local_folder']}")
    # Preprocess the datase
    big_table = file_name + '.csv'
    sql_table = big_table_name
    target_value = 'dm_sales_qty'
    dataset_name = 'dataset_test'
    preprocess_promo(folder=folder, big_table=big_table, sql_table=sql_table,
                     target_value=target_value, dataset_name=dataset_name)

    # Train the model
    desc = 'promo_test'
    data_name = dataset_name + '.pkl'
    target_value = 'dm_sales_qty'
    date_stop_train = config['date_stop_train']
    learning_rate = 0.3

    folder_name = train(desc=desc, folder=folder, data_name=data_name, target_value=target_value,
                        learning_rate=learning_rate, date_stop_train=date_stop_train)

    # Make a prediction
    prediction(folder_name, folder, data_name,
               target_value, learning_rate, date_stop_train)

    # save csv as table 
    print('saving csv as table')
    file_list = os.listdir(config['local_folder'])

    for f in file_list:
        if desc in f:
            result_file_name = f
    result_csv_path = f"{config['local_folder']}/{result_file_name}/promo_sales_order_prediction_by_item_store_dm.csv"
    os.system(f"hadoop fs -rm promo_sales_order_prediction_by_item_store_dm.csv")
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

    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(
        f"promo_sales_order_prediction_by_item_store_dm.csv")
    spark_df = spark_df.withColumn("item_id", spark_df["item_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("sub_id", spark_df["sub_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("current_dm_theme_id", spark_df["current_dm_theme_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("store_code", lpad(spark_df['store_code'], 3, '0'))
    spark_df = spark_df.withColumn("sales", spark_df["sales"].cast(FloatType()))
    spark_df = spark_df.withColumn("sales_prediction", spark_df["sales_prediction"].cast(FloatType()))
    spark_df = spark_df.withColumn("squared_error_predicted", spark_df["squared_error_predicted"].cast(FloatType()))
    spark_df = spark_df.withColumn("std_dev_predicted", spark_df["std_dev_predicted"].cast(FloatType()))
    spark_df = spark_df.withColumn("confidence_interval_max", spark_df["confidence_interval_max"].cast(FloatType()))
    spark_df = spark_df.withColumn("order_prediction", spark_df["order_prediction"].cast(FloatType()))
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database']}.promo_sales_order_prediction_by_item_store_dm")
    spark.stop()

    sql = f""" invalidate metadata {config['database']}.promo_sales_order_prediction_by_item_store_dm """
    impalaexec(sql)
    print('csv saved in the table')


if __name__ == '__main__':
    main()
