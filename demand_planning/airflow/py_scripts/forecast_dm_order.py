import datetime
import os
from datetime import timedelta
from os.path import abspath

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_current_time():
    return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")


def print_output(output_line):
    print(get_current_time(), output_line)


def insert_script_run(date_str, status, parameter, output_str, info_str, error_str, sqlc):
    sql = \
        """
        insert into vartefact.forecast_script_runs
        values(now(), '{0}', '{1}', 'Forecast process for DM', 'Order run', 
        '{2}', '{3}', '{4}', '{5}')
        """.replace("\n", " ")

    sql = sql.format(date_str, status, parameter, output_str, info_str, error_str)
    sqlc.sql(sql)


def dm_order_process(date_str):
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    print_output(f'\n\n\n Forecast process for DM start with input date {date_str} \n\n\n')

    # for logging
    output_str = ""
    info_str = f"Job start:{get_current_time()}, "

    spark = SparkSession.builder \
        .appName("Forecast process for DM") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '8g') \
        .config("spark.executor.memory", '8g') \
        .config("spark.num.executors", '8') \
        .config("hive.exec.compress.output", 'false') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext

    sqlc = SQLContext(sc)
    sqlc.setConf("hive.support.concurrency", "true")
    sqlc.setConf("hive.exec.parallel", "true")
    sqlc.setConf("hive.exec.dynamic.partition", "true")
    sqlc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlc.setConf("hive.exec.max.dynamic.partitions", "4096")
    sqlc.setConf("hive.exec.max.dynamic.partitions.pernode", "4096")

    print_output('Spark environment loaded')

    run_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()

    # starting day of the DM calculation period
    start_date = run_date + timedelta(weeks=2)

    # end day of the DM calculation period
    end_date = run_date + timedelta(weeks=7)

    stock_date = run_date + timedelta(days=-1)

    parameter = "Run date:" + start_date.strftime("%Y%m%d") \
                + ", DM start date:" + start_date.strftime("%Y%m%d") \
                + ", DM end date:" + end_date.strftime("%Y%m%d")

    spark.read.format('org.apache.kudu.spark.kudu') \
        .option('kudu.master', "dtla1apps11:7051,dtla1apps12:7051,dtla1apps13:7051") \
        .option('kudu.table', "impala::nsa.dm_extract_log") \
        .load() \
        .registerTempTable('dm_extract_log')

    print_output(f"Load DM items and stores for range {start_date} to {end_date}")

    dm_item_store_sql = \
        """
        SELECT distinct ndt.dm_theme_id,
            ndt.theme_start_date,
            ndt.theme_end_date,
            del.npp,
            del.ppp,
            del.ppp_start_date,
            del.ppp_end_date,
            del.city_code,
            fsd.store_code,
            fsd.dept_code,
            fsd.dept,
            icis.item_id,
            icis.sub_id,
            icis.item_code,
            icis.sub_code,
            icis.date_key AS run_date,
            fdo.first_order_date AS past_result
        FROM dm_extract_log del
        JOIN ods.nsa_dm_theme ndt ON del.dm_theme_id = ndt.dm_theme_id
        JOIN ods.p4md_stogld ps ON del.city_code = ps.stocity
        JOIN vartefact.forecast_stores_dept fsd ON ps.stostocd = fsd.store_code
        JOIN vartefact.forecast_item_code_id_stock icis ON icis.date_key = '{0}'
            AND del.item_code = CONCAT (
                icis.dept_code,
                icis.item_code
                )
            AND del.sub_code = icis.sub_code
            AND fsd.dept_code = icis.dept_code
            AND fsd.store_code = icis.store_code
        LEFT JOIN vartefact.forecast_dm_orders fdo ON ndt.dm_theme_id = fdo.dm_theme_id
            AND icis.dept_code = fdo.dept_code
            AND icis.item_code = fdo.item_code
            AND icis.sub_code = fdo.sub_code
            AND icis.store_code = fdo.store_code
        WHERE del.extract_order = 50
            AND ndt.theme_start_date >= '{1}'
            AND ndt.theme_end_date <= '{2}'
        """.replace("\n", " ")

    dm_item_store_sql = dm_item_store_sql.format(stock_date.strftime("%Y%m%d"), start_date.isoformat(),
                                                 end_date.isoformat())

    # # Exclude the DM that already have orders

    dm_item_store_df = sqlc.sql(dm_item_store_sql)

    print_output(f"Number of DM item stores in date range {dm_item_store_df.count()}")

    print_output("Exclude the DM that already have orders")

    dm_item_store_df = dm_item_store_df.filter("past_result is null")

    output_line = f"After filtering already calculated DM {dm_item_store_df.count()}"

    print_output(output_line)
    output_str = output_str + output_line + ","

    # # Only consider the nearest DM

    first_dm = dm_item_store_df. \
        groupBy(['item_id', 'sub_id', 'store_code']). \
        agg(F.min("theme_start_date").alias("theme_start_date"))

    dm_item_store_df = dm_item_store_df.join(first_dm, ['item_id', 'sub_id', 'store_code', 'theme_start_date'])

    dm_item_store_cnt = dm_item_store_df.count()

    print_output(f"After getting only first DM {dm_item_store_cnt}")
    output_str = output_str + f"After getting only first DM {dm_item_store_cnt}," + ","

    if dm_item_store_cnt == 0:
        print_output(f"skip date {date_str} cause no active order opportunity for today")
        info_str = info_str + f"Job Finish:{get_current_time()},"
        info_str = info_str + f"skip date {date_str} cause no active order opportunity for today"
        insert_script_run(date_str, "Success", parameter, output_str, info_str, "", sqlc)
        return

    dm_item_store_df.write.mode("overwrite").format("parquet").saveAsTable("vartefact.tmp_dm_item_store")

    dm_item_store_df.createOrReplaceTempView("dm_item_store")

    # # The first order day within PPP period
    print_output("Get first order day within PPP period")
    onstock_order_sql = \
        """
        SELECT dis.item_id,
            dis.sub_id,
            dis.store_code,
            id.pcb,
            id.dc_supplier_code,
            id.ds_supplier_code,
            id.rotation,
            ord.date_key AS first_order_date,
            dev.date_key AS first_delivery_date
        FROM dm_item_store dis
        JOIN vartefact.forecast_item_details id ON id.item_code = dis.item_code
            AND id.sub_code = dis.sub_code
            AND id.dept_code = dis.dept_code
        JOIN vartefact.onstock_order_delivery_mapping mp ON mp.dept_code = dis.dept_code
            AND id.rotation = mp.`class`
            AND mp.store_code = dis.store_code
        JOIN vartefact.forecast_calendar ord ON ord.weekday_short = mp.order_weekday
        JOIN vartefact.forecast_calendar dev ON dev.weekday_short = mp.delivery_weekday
            AND dev.week_index = ord.week_index + mp.week_shift
        WHERE to_timestamp(ord.date_key, 'yyyyMMdd') >= to_timestamp(dis.ppp_start_date, 'yyyy-MM-dd')
            AND dev.date_key <= '{0}'
        """.replace("\n", " ")

    onstock_order_sql = onstock_order_sql.format(end_date.strftime("%Y%m%d"))

    onstock_order_deliver_df = sqlc.sql(onstock_order_sql)

    xdock_order_sql = \
        """
        SELECT dis.item_id,
            dis.sub_id,
            dis.store_code,
            id.pcb,
            id.dc_supplier_code,
            id.ds_supplier_code,
            id.rotation,
            ord.date_key AS first_order_date,
            dev.date_key AS first_delivery_date
        FROM dm_item_store dis
        JOIN vartefact.forecast_item_details id ON id.item_code = dis.item_code
            AND id.sub_code = dis.sub_code
            AND id.dept_code = dis.dept_code
        JOIN vartefact.xdock_order_delivery_mapping xo ON dis.item_code = xo.item_code
            AND dis.sub_code = xo.sub_code
            AND dis.dept_code = xo.dept_code
        JOIN vartefact.forecast_calendar ord ON ord.iso_weekday = xo.order_weekday
        JOIN vartefact.forecast_calendar dev ON dev.iso_weekday = xo.delivery_weekday
            AND dev.week_index = (ord.week_index + xo.week_shift)
        WHERE to_timestamp(ord.date_key, 'yyyyMMdd') >= to_timestamp(dis.ppp_start_date, 'yyyy-MM-dd')
            AND dev.date_key <= '{0}'
        """.replace("\n", " ")

    xdock_order_sql = xdock_order_sql.format(end_date.strftime("%Y%m%d"))

    xdock_order_deliver_df = sqlc.sql(xdock_order_sql)

    order_deliver_df = onstock_order_deliver_df.union(xdock_order_deliver_df)

    first_order_df = order_deliver_df.groupBy(['item_id', 'sub_id', 'store_code']). \
        agg(F.min("first_order_date").alias("first_order_date"))

    first_order_deliver_df = order_deliver_df \
        .select(['item_id', 'sub_id', 'store_code', 'pcb', 'dc_supplier_code', 'ds_supplier_code',
                 'rotation', 'first_order_date', 'first_delivery_date']) \
        .join(first_order_df, ['item_id', 'sub_id', 'store_code', 'first_order_date'])

    dm_item_store_order_df = dm_item_store_df \
        .join(first_order_deliver_df, \
              ['item_id', 'sub_id', 'store_code'])

    dm_item_store_order_df.createOrReplaceTempView("dm_item_store_order")

    output_line = f"Number of item stores that will have DM {dm_item_store_order_df.count()}"
    print_output(output_line)
    output_str = output_str + output_line + ","

    # # Get DM sales prediction

    dm_sales_predict_sql = \
        """
        SELECT dm.*,
            cast(pred.sales_prediction AS DOUBLE) AS dm_sales
        FROM temp.v_forecast_dm_sales_prediction pred
        JOIN dm_item_store_order dm ON cast(pred.item_id AS INT) = dm.item_id
            AND cast(pred.sub_id AS INT) = dm.sub_id
            AND cast(pred.current_dm_theme_id AS INT) = dm.dm_theme_id
            AND pred.store_code = dm.store_code
        """.replace("\n", " ")

    dm_prediction = sqlc.sql(dm_sales_predict_sql)

    dm_prediction.createOrReplaceTempView("dm_prediction")

    dm_prediction.write.mode("overwrite").format("parquet").saveAsTable("vartefact.tmp_dm_prediction")

    output_line = f"Number of DM sales prediction {dm_prediction.count()}"
    print_output(output_line)
    output_str = output_str + output_line + ","

    # # Get store stock level
    actual_stock_sql = \
        """
        SELECT icis.item_id,
            icis.sub_id,
            icis.store_code,
            dp.rotation,
            cast(icis.balance_qty AS DOUBLE) current_store_stock,
            cast(stp.shelf_capacity as double) as shelf_capacity,
            cast(opi.ittreplentyp as int) as ittreplentyp,
            cast(opi.ittminunit as double) as ittminunit
        FROM dm_prediction dp
        JOIN vartefact.forecast_item_code_id_stock icis ON icis.item_id = dp.item_id
            AND icis.sub_id = dp.sub_id
            AND icis.store_code = dp.store_code
            AND icis.date_key = {0}
        JOIN ods.p4md_itmsto opi ON cast(opi.ittitmid AS INT) = dp.item_id
            AND opi.ittstocd = dp.store_code
        JOIN vartefact.forecast_p4cm_store_item stp ON icis.item_code = stp.item_code
            AND dp.sub_code = stp.sub_code
            AND dp.dept_code = stp.dept_code
            AND dp.store_code = stp.store_code
            AND stp.date_key = '{1}'
        """.replace("\n", " ")

    actual_stock_sql = actual_stock_sql.format(stock_date.strftime("%Y%m%d"), run_date.strftime("%Y%m%d"))

    actual_stock = sqlc.sql(actual_stock_sql)

    actual_stock = \
        actual_stock.withColumn("initial_minumum_stock",
                                F.when((actual_stock.ittreplentyp == 1) | (actual_stock.ittreplentyp == 4),
                                       actual_stock.shelf_capacity)
                                .when(actual_stock.ittreplentyp == 3,
                                      actual_stock.ittminunit))

    actual_stock = \
        actual_stock.withColumn("minumum_stock",
                                F.when((actual_stock.initial_minumum_stock == 0)
                                       | F.isnull(actual_stock.initial_minumum_stock),
                                       F.when(actual_stock.rotation == 'A', 8)
                                       .when(actual_stock.rotation == 'B', 6)
                                       .when(actual_stock.rotation == 'X', 2))
                                .otherwise(actual_stock.initial_minumum_stock))

    actual_stock = actual_stock.drop('initial_minumum_stock', 'rotation')

    output_line = f"Number of current stock stores {actual_stock.count()}"
    print_output(output_line)
    output_str = output_str + output_line + ","

    dm_with_stock = dm_prediction.join(actual_stock, ['item_id', 'sub_id', 'store_code'], "left")

    # # Regular sales before first delivery day

    regular_sales_sql = \
        """
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            daily_sales_prediction AS sales_prediction
        FROM temp.v_forecast_daily_sales_prediction fcst
        JOIN dm_prediction dp ON fcst.item_id = dp.item_id
            AND fcst.sub_id = dp.sub_id
            AND fcst.store_code = dp.store_code
            AND fcst.date_key >= {0}
            AND to_timestamp(fcst.date_key, 'yyyyMMdd') <= to_timestamp(dp.first_delivery_date, 'yyyy-MM-dd')
        """.replace("\n", " ")

    regular_sales_sql = regular_sales_sql.format(run_date.strftime("%Y%m%d"))
    # -

    regular_sales = sqlc.sql(regular_sales_sql)

    agg_regular_sales = regular_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
        agg(F.sum("sales_prediction").alias("sales_before_order"))

    dm_with_sales = dm_with_stock.join(agg_regular_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

    # # Orders to be received before first order day
    print_output("Arriving stock")

    orders_received_sql = \
        """
        SELECT *
        FROM (
            SELECT dp.item_id,
                dp.sub_id,
                dp.store_code,
                dp.dm_theme_id,
                cast(foo.order_qty AS DOUBLE) order_qty
            FROM vartefact.forecast_onstock_orders foo
            JOIN dm_prediction dp ON foo.item_id = dp.item_id
                AND foo.sub_id = dp.sub_id
                AND foo.store_code = dp.store_code
                AND foo.order_day >= {0}
                AND to_timestamp(foo.delivery_day, 'yyyyMMdd') <= to_timestamp(dp.first_delivery_date, 'yyyy-MM-dd')
            
            UNION
            
            SELECT dp.item_id,
                dp.sub_id,
                dp.store_code,
                dp.dm_theme_id,
                cast(fxo.order_qty AS DOUBLE) order_qty
            FROM vartefact.forecast_xdock_orders fxo
            JOIN dm_prediction dp ON fxo.item_id = dp.item_id
                AND fxo.sub_id = dp.sub_id
                AND fxo.store_code = dp.store_code
                AND fxo.order_day >= {0}
                AND to_timestamp(fxo.delivery_day, 'yyyyMMdd') <= to_timestamp(dp.first_delivery_date, 'yyyy-MM-dd')
            ) t
        """.replace("\n", " ")

    orders_received_sql = orders_received_sql.format(run_date.strftime("%Y%m%d"))
    # -

    orders_received = sqlc.sql(orders_received_sql)

    agg_orders_received = orders_received.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
        agg(F.sum("order_qty").alias("order_received"))

    dm_with_orders = dm_with_sales.join(agg_orders_received, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

    # # Regular sales from first order day to DM start day
    print_output("Regular sales before DM")

    dm_regular_sales_sql = \
        """
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            daily_sales_prediction AS sales_prediction
        FROM temp.v_forecast_daily_sales_prediction fcst
        JOIN dm_prediction dp ON fcst.item_id = dp.item_id
            AND fcst.sub_id = dp.sub_id
            AND fcst.store_code = dp.store_code
            AND fcst.date_key > dp.first_delivery_date
            AND to_timestamp(fcst.date_key, 'yyyyMMdd') < to_timestamp(dp.theme_start_date, 'yyyy-MM-dd')
        """.replace("\n", " ")

    dm_regular_sales = sqlc.sql(dm_regular_sales_sql)

    agg_dm_regular_sales = dm_regular_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
        agg(F.sum("sales_prediction").alias("regular_sales_before_dm"))

    dm_with_regular = dm_with_orders.join(agg_dm_regular_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'],
                                          "left")

    # # For ppp <= 90% npp, get 4 weeks after sales for ROTATION A items
    print_output("DM PPP logic")

    after_fourweek_sql = \
        """
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            fcst.daily_sales_prediction AS sales_prediction
        FROM dm_prediction dp
        JOIN temp.v_forecast_daily_sales_prediction fcst ON fcst.item_id = dp.item_id
            AND fcst.sub_id = dp.sub_id
            AND fcst.store_code = dp.store_code
            AND to_timestamp(fcst.date_key, 'yyyyMMdd') > to_timestamp(dp.theme_end_date, 'yyyy-MM-dd')
            AND to_timestamp(fcst.date_key, 'yyyyMMdd') < date_add(to_timestamp(dp.theme_end_date, 'yyyy-MM-dd'), 28)
        WHERE dp.rotation = 'A'
            AND dp.ppp <= dp.npp * 0.9
        """.replace("\n", " ")

    after_fourweek_sales = sqlc.sql(after_fourweek_sql.format(run_date.strftime("%Y%m%d")))

    agg_after_fourweek_sales = after_fourweek_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
        agg(F.sum("sales_prediction").alias("four_weeks_after_dm"))

    output_line = f"Number of DM having PPP {agg_after_fourweek_sales.count()}"
    print_output(output_line)
    output_str = output_str + output_line + ","

    dm_with_fourweek = dm_with_regular.join(agg_after_fourweek_sales,
                                            ['item_id', 'sub_id', 'store_code', 'dm_theme_id'],
                                            "left")

    # # Fill NA

    dm_with_fourweek = dm_with_fourweek.na.fill(0)
    dm_with_fourweek.cache()

    output_line = f"Number of DM order {dm_with_fourweek.count()}"
    print_output(output_line)
    output_str = output_str + output_line

    dm_with_fourweek.write.mode("overwrite").format("parquet").saveAsTable("vartefact.tmp_dm_before_final")

    # # Final calculation

    print_output("Calculate order quantity")
    dm_final = dm_with_fourweek.withColumn("dm_order_qty_without_pcb", dm_with_fourweek.sales_before_order
                                           + dm_with_fourweek.regular_sales_before_dm
                                           + dm_with_fourweek.four_weeks_after_dm
                                           + dm_with_fourweek.dm_sales
                                           + dm_with_fourweek.minumum_stock
                                           - dm_with_fourweek.order_received
                                           - dm_with_fourweek.current_store_stock)

    dm_final = dm_final.withColumn("true_pcb",
                                   F.when((dm_final.dc_supplier_code == 'KSSE') | (dm_final.dc_supplier_code == 'KXS1'),
                                          1)
                                   .otherwise(dm_final.pcb))

    dm_final_pcb = dm_final \
        .withColumn("dm_order_qty",
                    F.when(dm_final.dm_order_qty_without_pcb > 0.0,
                           F.ceil(dm_final.dm_order_qty_without_pcb / dm_final.true_pcb) * dm_final.true_pcb)
                    .otherwise(int(0)))

    dm_final_pcb.createOrReplaceTempView("dm_final_pcb")

    print_output("Write order to datalake")
    dm_sql = \
        """
        INSERT INTO vartefact.forecast_dm_orders 
        PARTITION (
            dm_theme_id)
        SELECT 
        item_id,
            sub_id,
            store_code,
            theme_start_date,
            theme_end_date,
            npp,
            ppp,
            ppp_start_date,
            ppp_end_date,
            city_code,
            dept_code,
            dept,
            item_code,
            sub_code,
            pcb,
            dc_supplier_code,
            ds_supplier_code,
            rotation,
            run_date,
            first_order_date,
            first_delivery_date,
            sales_before_order,
            order_received,
            regular_sales_before_dm,
            four_weeks_after_dm,
            dm_sales,
            current_store_stock,
            dm_order_qty,
            dm_order_qty_without_pcb,
            dm_theme_id
        FROM dm_final_pcb
        """.replace("\n", " ")

    sqlc.sql(dm_sql)

    sqlc.sql("refresh table vartefact.forecast_dm_orders")

    info_str = info_str + f"Job Finish:{get_current_time()}"
    insert_script_run(date_str, "Success", parameter, output_str, info_str, "", sqlc)

    sc.stop()
    print_output("Job finish")
