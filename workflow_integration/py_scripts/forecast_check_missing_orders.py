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
            when icis.item_id is null 
                then 'fds.p4cm_daily_stock not loaded at job run time'
            when stp.item_code is null 
                then 'ods.p4cm_store_item not loaded at job run time'
            else 'Others'
            END as reason
        from
            vartefact.v_forecast_daily_onstock_order_items itm
            left join vartefact.forecast_onstock_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.store_code = ords.store_code
            and ords.order_day = '{0}'
            left join vartefact.forecast_item_code_id_stock icis on itm.dept_code = icis.dept_code
            and itm.item_code = icis.item_code
            and itm.sub_code = icis.sub_code
            and itm.store_code = icis.store_code
            and icis.date_key = '{1}'
            left join vartefact.forecast_p4cm_store_item stp
            on itm.item_code = stp.item_code
            and itm.sub_code = stp.sub_code
            and itm.store_code = stp.store_code
            and itm.dept_code = stp.dept_code
            and stp.date_key = '{0}'
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
            when icis.item_id is null 
                then 'fds.p4cm_daily_stock not loaded at job run time'
            when stp.item_code is null 
                then 'ods.p4cm_store_item not loaded at job run time'
            else 'Others'
            END as reason
        from
            vartefact.v_forecast_daily_xdock_order_items itm
            left join vartefact.forecast_xdock_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.store_code = ords.store_code
            and ords.order_day = '{0}'
            left join vartefact.forecast_item_code_id_stock icis on itm.dept_code = icis.dept_code
            and itm.item_code = icis.item_code
            and itm.sub_code = icis.sub_code
            and itm.store_code = icis.store_code
            and icis.date_key = '{1}'
            left join vartefact.forecast_p4cm_store_item stp
            on itm.item_code = stp.item_code
            and itm.sub_code = stp.sub_code
            and itm.store_code = stp.store_code
            and itm.dept_code = stp.dept_code
            and stp.date_key = '{0}'
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
 
    wb = Workbook()
    ws = wb.active
    
    print(f'Total {len(missing_store_orders_df)} missing store orders found')
    
    if len(missing_store_orders_df) == 0:
        ws.append(["No missing orders found. All item stores have orders generated."])
    
    else:
        ws.append(['rotation', 'dept_code', 'item_code', 'sub_code', 
                   'cn_name', 'store_code', 'con_holding', 'ds_supplier_code', 
                   'dc_supplier_code', 'reason'])
        
        for index, row in missing_store_orders_df.iterrows():
            ws.append([row.rotation, row.dept_code, row.item_code, row.sub_code,
                       row.cn_name, row.store_code, row.con_holding, row.ds_supplier_code,
                       row.dc_supplier_code, row.reason])
            
            print(f'{row.dept_code}, {row.item_code}, {row.sub_code}, {row.store_code}, {row.dc_supplier_code}, {row.reason}')
            
    wb.save(record_folder + '/order_checks/' + store_missing_file)

    wb.save(output_path + '/' + store_missing_file)

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
            when icis.item_id is null 
                then 'Missing stock in table fds.p4cm_daily_stock'
            when ldd.stock_available_sku is null 
                then 'Missing stock in table lfms.daily_dcstock'
            when fdls.avg_sales_qty is null 
                then 'Missing average sales in table lfms.ord'
            else 'Others'
            END as reason
        from
            vartefact.v_forecast_inscope_dc_item_details itm
        JOIN vartefact.forecast_dc_order_delivery_mapping dodm
            on dodm.risk_item_unilever = itm.risk_item_unilever
            AND dodm.con_holding = itm.holding_code
            AND dodm.order_date = '{0}'
        JOIN vartefact.forecast_calendar ord
            on ord.date_key = dodm.order_date
        LEFT OUTER JOIN vartefact.forecast_dc_latest_sales fdls ON fdls.item_code = itm.item_code
            AND fdls.sub_code = itm.sub_code
            AND fdls.dept_code = itm.dept_code
        LEFT join vartefact.forecast_dc_orders ords on itm.dept_code = ords.dept_code
            and itm.item_code = ords.item_code
            and itm.sub_code = ords.sub_code
            and itm.holding_code = ords.con_holding
            and ords.order_day = '{0}'
        left join 
            (
                select distinct dept_code, item_code, sub_code, item_id, sub_id
                FROM vartefact.forecast_item_code_id_stock
                where date_key = '{1}'
            ) icis on itm.dept_code = icis.dept_code
            and itm.item_code = icis.item_code
            and itm.sub_code = icis.sub_code
        LEFT join vartefact.forecast_lfms_daily_dcstock ldd on icis.item_id = ldd.item_id
            and icis.sub_id = ldd.sub_id
            and ldd.date_key = '{1}'
            and ldd.dc_site = 'DC1'
        where
            ords.order_qty is null
            AND itm.rotation != 'X'
        order by
            itm.dept_code,
            itm.item_code,
            itm.sub_code
        """.replace("\n", " ")

    dc_order_check_sql = dc_order_check_sql.format(check_date.strftime("%Y%m%d"), stock_date.strftime("%Y%m%d"))

    missing_dc_order = sqlc.sql(dc_order_check_sql)

    missing_dc_order_df = missing_dc_order.toPandas()
    
    wb = Workbook()
    ws = wb.active
    
    print(f'Total {len(missing_dc_order_df)} missing DC orders found')
    
    if len(missing_dc_order_df) == 0:
        ws.append(["No missing orders found. All items in DC have orders generated."])
    
    else:
        ws.append(['rotation', 'dept_code', 'item_code', 'sub_code', 
                   'item_name_local', 'current_warehouse', 'holding_code', 'primary_ds_supplier', 
                   'reason'])
        
        for index, row in missing_dc_order_df.iterrows():
            ws.append([row.rotation, row.dept_code, row.item_code, row.sub_code,
                       row.item_name_local, row.current_warehouse, row.holding_code, row.primary_ds_supplier,
                       row.reason])
            
            print(f'{row.dept_code}, {row.item_code}, {row.sub_code}, {row.current_warehouse}, {row.primary_ds_supplier}, {row.reason}')
            
    wb.save(record_folder + '/order_checks/' + dc_missing_file)

    wb.save(output_path + '/' + dc_missing_file)

    sc.stop()
