import calendar
import datetime
import os
from datetime import timedelta
from os.path import abspath

import numpy as np
import pandas as pd
from openpyxl import Workbook
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


def store_missing_order_process(date_str, record_folder, output_path, store_missing_file):
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    spark = SparkSession.builder \
        .appName("Generate store order file") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '8g') \
        .config("spark.executor.memory", '8g') \
        .config("spark.num.executors", '8') \
        .config("hive.exec.compress.output", 'false') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext

    sqlc = SQLContext(sc)

    check_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()

    stock_date = check_date + timedelta(days=-1)

    onstock_order_check_sql = \
        """
        select
            itm.*,
            ords.order_qty,
            CASE 
            when sto.balance_qty is null 
                then 'Missing stock in table fds.p4cm_daily_stock'
            else 'Others'
            END as reason
        from
            vartefact.v_forecast_daily_onstock_order_items itm
            left join vartefact.forecast_onstock_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.store_code = ords.store_code
            and ords.order_day = '{0}'
            left join fds.p4cm_daily_stock sto on itm.dept_code = sto.dept_code
            and itm.item_code = sto.item_code
            and itm.sub_code = sto.sub_code
            and itm.store_code = sto.store_code
            and sto.date_key = '{1}'
        where
            itm.order_day = '{0}'
            and ords.order_qty is null
        order by
            itm.dept_code,
            itm.item_code,
            itm.sub_code,
            itm.store_code
        """.replace("\n", " ")

    onstock_order_check_sql = onstock_order_check_sql.format(check_date.strftime("%Y%m%d"),
                                                             stock_date.strftime("%Y%m%d"))

    missing_onstock_orders = sqlc.sql(onstock_order_check_sql)

    xdock_order_check_sql = \
        """
        select
            itm.*,
            ords.order_qty,
            CASE 
            when sto.balance_qty is null 
                then 'Missing stock in table fds.p4cm_daily_stock'
            else 'Others'
            END as reason
        from
            vartefact.v_forecast_daily_xdock_order_items itm
            left join vartefact.forecast_xdock_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.store_code = ords.store_code
            and ords.order_day = '{0}'
            left join fds.p4cm_daily_stock sto on itm.dept_code = sto.dept_code
            and itm.item_code = sto.item_code
            and itm.sub_code = sto.sub_code
            and itm.store_code = sto.store_code
            and sto.date_key = '{1}'
        where
            itm.order_day = '{0}'
            and ords.order_qty is null
        order by
            itm.dept_code,
            itm.item_code,
            itm.sub_code,
            itm.store_code
        """.replace("\n", " ")

    missing_xdock_orders = sqlc.sql(xdock_order_check_sql)

    missing_store_orders = missing_onstock_orders.union(missing_xdock_orders)

    missing_store_orders_df = missing_store_orders.toPandas()

    missing_orders = missing_store_orders_df[['rotation', 'dept_code', 'item_code', 'sub_code', 'cn_name', 'store_code',
                                              'con_holding', 'ds_supplier_code', 'dc_supplier_code', 'reason']]

    if len(missing_store_orders_df) == 0:
        missing_orders = pd.DataFrame(["No missing orders found. All item stores have orders generated."])
        missing_orders.to_csv(record_folder + '/' + store_missing_file, mode='w', index=False, header=False)
        missing_orders.to_csv(output_path + '/' + store_missing_file, mode='w', index=False, header=False)
    else:
        missing_orders.columns = ['Rotation', 'Dept_Code', 'Item_Code', 'CN_Name', 'Sub_code', 'Store_Code',
                                  'Con_holding', 'DS_supplier', 'DC_supplier', 'Missing_reason']
        missing_orders.to_csv(record_folder + '/' + store_missing_file, mode='w', index=False, header=True)
        missing_orders.to_csv(output_path + '/' + store_missing_file, mode='w', index=False, header=True)

    sc.stop()


def dc_missing_order_process(date_str, record_folder, output_path, dc_missing_file):
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    spark = SparkSession.builder \
        .appName("Generate store order file") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '8g') \
        .config("spark.executor.memory", '8g') \
        .config("spark.num.executors", '8') \
        .config("hive.exec.compress.output", 'false') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext

    sqlc = SQLContext(sc)

    check_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()

    stock_date = check_date + timedelta(days=-1)

    dc_order_check_sql = \
        """
        select
            distinct
            itm.*,
            ords.order_qty,
            CASE 
            when sto.balance_qty is null 
                then 'Missing stock in table fds.p4cm_daily_stock'
            when ldd.stock_available_sku is null 
                then 'Missing stock in table lfms.daily_dcstock'
            else 'Others'
            END as reason
        from
            vartefact.forecast_dc_latest_sales fdls
        JOIN vartefact.forecast_dc_order_delivery_mapping dodm 
            ON dodm.con_holding = fdls.con_holding
            AND dodm.order_date = '{0}'
        JOIN vartefact.v_forecast_inscope_dc_item_details itm ON fdls.item_code = itm.item_code
            AND fdls.sub_code = itm.sub_code
            AND fdls.dept_code = itm.dept_code
            AND dodm.risk_item_unilever = itm.risk_item_unilever
            AND itm.rotation != 'X'
        JOIN vartefact.forecast_calendar ord
            on ord.date_key = dodm.order_date
        LEFT join vartefact.forecast_dc_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.holding_code = ords.con_holding
            and ords.order_day = '{0}'
        LEFT join fds.p4cm_daily_stock sto on itm.dept_code = sto.dept_code
            and itm.item_code = sto.item_code
            and itm.sub_code = sto.sub_code
            and sto.date_key = '{1}'
        LEFT join vartefact.forecast_lfms_daily_dcstock ldd on sto.item_id = ldd.item_id
            and sto.sub_id = ldd.sub_id
            and ldd.date_key = '{1}'
            and ldd.dc_site = 'DC1'
        where
            ords.order_qty is null
        order by
            itm.dept_code,
            itm.item_code,
            itm.sub_code
        """.replace("\n", " ")

    dc_order_check_sql = dc_order_check_sql.format(check_date.strftime("%Y%m%d"), stock_date.strftime("%Y%m%d"))

    missing_dc_order = sqlc.sql(dc_order_check_sql)

    missing_dc_order_df = missing_dc_order.toPandas()

    missing_orders = missing_dc_order_df[['rotation', 'dept_code', 'item_code', 'sub_code', 'item_name_local',
                                          'current_warehouse', 'holding_code', 'primary_ds_supplier', 'reason']]

    if len(missing_dc_order_df) == 0:
        missing_orders = pd.DataFrame(["No missing orders found. All item stores have orders generated."])
        missing_orders.to_csv(record_folder + '/' + dc_missing_file, mode='w', index=False, header=False)
        missing_orders.to_csv(output_path + '/' + dc_missing_file, mode='w', index=False, header=False)
    else:
        missing_orders.columns = ['Rotation', 'Dept_Code', 'Item_Code', 'CN_Name', 'Sub_code', 'Warehouse',
                                  'Con_holding', 'DS_supplier', 'Missing_reason']

        missing_orders.to_csv(record_folder + '/' + dc_missing_file, mode='w', index=False, header=True)
        missing_orders.to_csv(output_path + '/' + dc_missing_file, mode='w', index=False, header=False)

    sc.stop()
