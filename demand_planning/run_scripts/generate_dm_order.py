# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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

# +
import datetime
from datetime import timedelta

from load_spark import load_spark
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

spark = load_spark("generate_dm_order")

sc = spark.sparkContext

sqlc = SQLContext(sc)

# +
print(str(datetime.datetime.now()), "start")
# The input
date_str = '2019-01-08'

# safety stock level
safety_stock_level = 2;

run_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

# starting day of the DM calculation period
start_date = run_date + timedelta(weeks=2)

# end day of the DM calculation period
end_date = run_date + timedelta(weeks=7)

stock_date = run_date + timedelta(days=-1)

# +
#item_id = 1056574
item_id = None

# sub_id = 1431061
sub_id = None

# store_code = "150"
store_code = None

dm_theme_id = None
# -

spark.read.format('org.apache.kudu.spark.kudu') \
    .option('kudu.master', "dtla1apps11:7051,dtla1apps12:7051,dtla1apps13:7051") \
    .option('kudu.table', "impala::nsa.dm_extract_log") \
    .load() \
    .registerTempTable('dm_extract_log')

# +
dm_item_store_sql = \
    """
    select 
    ndt.dm_theme_id, ndt.theme_start_date, ndt.theme_end_date,
    del.npp, del.ppp, del.ppp_start_date, del.ppp_end_date, 
    del.city_code, fsd.store_code, fsd.dept_code, fsd.dept,
    icis.item_id, icis.sub_id, icis.item_code, icis.sub_code,
    icis.date_key as run_date,
    fdo.first_order_date as past_result
    from 
    vartefact.nsa_dm_extraction_log del
    join ods.nsa_dm_theme  ndt
        on del.dm_theme_id = ndt.dm_theme_id
    join ods.p4md_stogld ps
        on del.city_code = ps.stocity
    join vartefact.forecast_stores_dept fsd
        on ps.stostocd = fsd.store_code
    join vartefact.forecast_item_code_id_stock icis
        on icis.date_key = '{0}'
        AND del.item_code = concat(icis.dept_code, icis.item_code)
        AND del.sub_code = icis.sub_code
        AND fsd.dept_code = icis.dept_code
        AND fsd.store_code = icis.store_code
    left join vartefact.forecast_dm_orders fdo
        ON ndt.dm_theme_id = fdo.dm_theme_id
        AND icis.dept_code = fdo.dept_code
        AND icis.item_code = fdo.item_code
        AND icis.sub_code = fdo.sub_code
        AND icis.store_code = fdo.store_code
    where del.extract_order = 50
     and ndt.theme_start_date >= '{1}' 
     and ndt.theme_end_date <= '{2}'
    """.replace("\n", " ")

dm_item_store_sql = dm_item_store_sql.format(start_date.strftime("%Y%m%d"), start_date.isoformat(),
                                             end_date.isoformat())
# -

# # Exclude the DM that already have orders

# +
dm_item_store_df = sqlc.sql(dm_item_store_sql)

if item_id != None:
    dm_item_store_df = dm_item_store_df.filter(f"item_id={item_id}")

if sub_id != None:
    dm_item_store_df = dm_item_store_df.filter(f"sub_id={sub_id}")

if store_code != None:
    dm_item_store_df = dm_item_store_df.filter(f"store_code='{store_code}'")

if dm_theme_id != None:
    dm_item_store_df = dm_item_store_df.filter(f"dm_theme_id={dm_theme_id}")

print("Before filtering already calculated DM ", dm_item_store_df.count())

dm_item_store_df = dm_item_store_df.filter("past_result is null")

print("After filtering already calculated DM ", dm_item_store_df.count())
# -

# # Only consider the nearest DM

first_dm = dm_item_store_df. \
    groupBy(['item_id', 'sub_id', 'store_code']). \
    agg(F.min("theme_start_date").alias("theme_start_date"))

dm_item_store_df = dm_item_store_df.join(first_dm, ['item_id', 'sub_id', 'store_code', 'theme_start_date'])

print("After getting only first DM ", dm_item_store_df.count())

dm_item_store_df.write.mode("overwrite").format("parquet").saveAsTable("vartefact.tmp_dm_item_store")

sqlc.table("vartefact.tmp_dm_item_store").createOrReplaceTempView("dm_item_store")

# # The first order day within PPP period

# +
onstock_order_sql = \
    """
    SELECT 
        dis.item_id,
        dis.sub_id,
        dis.store_code,
        id.pcb,
        id.dc_supplier_code,
        id.ds_supplier_code,
        id.rotation,
        ord.date_key AS order_date,
        dev.date_key as delivery_date
    from dm_item_store dis
    join vartefact.forecast_item_details id 
        on id.item_code = dis.item_code
        and id.sub_code = dis.sub_code
        and id.dept_code = dis.dept_code
    join vartefact.ordinary_onstock_order_deliver_mapping mp
        on mp.dept = dis.dept 
        and id.rotation = mp.`class`
        and mp.store_code = dis.store_code
    join vartefact.forecast_calendar ord
        on ord.weekday_short = mp.order_weekday
    join  vartefact.forecast_calendar dev
        on dev.weekday_short = mp.deliver_weekday and dev.week_index = ord.week_index + mp.week_shift
    where to_date(ord.date_key, 'yyyyMMdd') >= to_date(dis.ppp_start_date, 'yyyy-MM-dd') and dev.date_key <='{0}'
    """.replace("\n", " ")

onstock_order_sql = onstock_order_sql.format(end_date.strftime("%Y%m%d"))

# +
onstock_order_deliver_df = sqlc.sql(onstock_order_sql)

if item_id != None:
    onstock_order_deliver_df = onstock_order_deliver_df.filter(f"item_id={item_id}")

if sub_id != None:
    onstock_order_deliver_df = onstock_order_deliver_df.filter(f"sub_id={sub_id}")

if store_code != None:
    onstock_order_deliver_df = onstock_order_deliver_df.filter(f"store_code='{store_code}'")
# -


onstock_first_order_df = onstock_order_deliver_df. \
    groupBy(['item_id', 'sub_id', 'store_code', 'pcb','dc_supplier_code', 'ds_supplier_code', 'rotation']). \
    agg(F.min("order_date").alias("first_order_date"), F.min("delivery_date").alias("first_delivery_date"))

# +
xdock_order_sql = \
    """
    SELECT 
        dis.item_id,
        dis.sub_id,
        dis.store_code,
        id.pcb,
        id.dc_supplier_code,
        id.ds_supplier_code,
        id.rotation,
        ord.date_key AS order_date,
        dev.date_key as delivery_date
    from dm_item_store dis
    join vartefact.forecast_item_details id 
        on id.item_code = dis.item_code
        and id.sub_code = dis.sub_code
        and id.dept_code = dis.dept_code
    join vartefact.ordinary_xrotation_order_deliver_mapping xo
        on dis.item_code = xo.item_code and dis.sub_code = xo.sub_code 
        and dis.dept_code = xo.dept_code
    join vartefact.forecast_calendar ord
        on ord.iso_weekday = xo.order_weekday
    join vartefact.forecast_calendar dev
        on dev.iso_weekday = xo.deliver_weekday and dev.week_index = ord.week_index + xo.week_shift
    where to_date(ord.date_key, 'yyyyMMdd') >= to_date(dis.ppp_start_date, 'yyyy-MM-dd') and dev.date_key <='{0}' 
    """.replace("\n", " ")

xdock_order_sql = xdock_order_sql.format(end_date.strftime("%Y%m%d"))

# +
xdock_order_deliver_df = sqlc.sql(xdock_order_sql)

if item_id is not None:
    xdock_order_deliver_df = xdock_order_deliver_df.filter(f"item_id={item_id}")

if sub_id is not None:
    xdock_order_deliver_df = xdock_order_deliver_df.filter(f"sub_id={sub_id}")

if store_code is not None:
    xdock_order_deliver_df = xdock_order_deliver_df.filter(f"store_code='{store_code}'")
# -

xdock_first_order_df = xdock_order_deliver_df. \
    groupBy(['item_id', 'sub_id', 'store_code', 'pcb','dc_supplier_code', 'ds_supplier_code', 'rotation']). \
    agg(F.min("order_date").alias("first_order_date"), F.min("delivery_date").alias("first_delivery_date"))

dm_item_store_order_df = dm_item_store_df \
    .join(onstock_first_order_df.union(xdock_first_order_df), \
          ['item_id', 'sub_id', 'store_code'])

dm_item_store_order_df.createOrReplaceTempView("dm_item_store_order")

print("Number of item stores that will have DM", dm_item_store_order_df.count())

# # Get DM sales prediction

dm_sales_predict_sql = \
    """
    
    select 
      dm.*,
      cast(pred.sales_prediction as double) as dm_sales
    from 
        vartefact.dm_pred_results_simple pred
        join dm_item_store_order dm
        on cast(pred.item_id as int) = dm.item_id
        and cast(pred.sub_id as int) = dm.sub_id
        and cast(pred.current_dm_theme_id as int) = dm.dm_theme_id
        and pred.store_code = dm.store_code
    """.replace("\n", " ")

dm_prediction = sqlc.sql(dm_sales_predict_sql)

dm_prediction.createOrReplaceTempView("dm_prediction")

dm_item_store_df.write.mode("overwrite").format("parquet").saveAsTable("vartefact.dm_prediction")

print("Number of item stores with DM prediction", dm_prediction.count())

# # Get store stock level

# +
actual_stock_sql = \
    """
    SELECT icis.item_id,
     icis.sub_id,
     icis.store_code,
     cast(icis.balance_qty AS DOUBLE) current_store_stock
    FROM dm_prediction dp
    left JOIN vartefact.item_code_id_stock icis
        on icis.item_id = dp.item_id
        and icis.sub_id = dp.sub_id
        and icis.store_code = dp.store_code 
        and icis.date_key = {0}
    """.replace("\n", " ")

actual_stock_sql =  actual_stock_sql.format(stock_date.strftime("%Y%m%d"))
# -

actual_stock = sqlc.sql(actual_stock_sql)

dm_with_stock = dm_prediction.join(actual_stock, ['item_id', 'sub_id', 'store_code'], "left")

# # Regular sales before first delivery day

# +
regular_sales_sql = \
    """
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            daily_sales_prediction as sales_prediction
        FROM
            vartefact.v_forecast_daily_sales_prediction fcst
        join dm_prediction dp
            on fcst.item_id = dp.item_id
            and fcst.sub_id = dp.sub_id
            and fcst.store_code = dp.store_code
            and fcst.date_key >= {0}
            and to_date(fcst.date_key, 'yyyyMMdd') <= to_date(dp.first_delivery_date, 'yyyy-MM-dd')
    """.replace("\n", " ")

regular_sales_sql = regular_sales_sql.format(run_date.strftime("%Y%m%d"))
# -

regular_sales = sqlc.sql(regular_sales_sql)

agg_regular_sales = regular_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
    agg(F.sum("sales_prediction").alias("sales_before_order"))

dm_with_sales = dm_with_stock.join(agg_regular_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

# # Orders to be received before first order day

# +
orders_received_sql = \
    """
    select * from (
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            cast(foo.order_qty as double) order_qty
        FROM
            vartefact.forecast_onstock_orders foo
        join dm_prediction dp
            on foo.item_id = dp.item_id
            and foo.sub_id = dp.sub_id
            and foo.store_code = dp.store_code
            and foo.order_day >= {0}
            and to_date(foo.delivery_day, 'yyyyMMdd') <= to_date(dp.first_delivery_date, 'yyyy-MM-dd')
        union
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            cast(fxo.order_qty as double) order_qty
        FROM
            vartefact.forecast_xdock_orders fxo
        join dm_prediction dp
            on fxo.item_id = dp.item_id
            and fxo.sub_id = dp.sub_id
            and fxo.store_code = dp.store_code
            and fxo.order_day >= {0}
            and to_date(fxo.delivery_day, 'yyyyMMdd') <= to_date(dp.first_delivery_date, 'yyyy-MM-dd')
    ) t
    """.replace("\n", " ")

orders_received_sql = orders_received_sql.format(run_date.strftime("%Y%m%d"))
# -

orders_received = sqlc.sql(orders_received_sql)

agg_orders_received = orders_received.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
    agg(F.sum("order_qty").alias("order_received"))

dm_with_orders = dm_with_sales.join(agg_orders_received, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

# # Regular sales from first order day to DM start day

dm_regular_sales_sql = \
    """
        SELECT dp.item_id,
            dp.sub_id,
            dp.store_code,
            dp.dm_theme_id,
            daily_sales_prediction as sales_prediction
        FROM
            vartefact.v_forecast_daily_sales_prediction fcst
        join dm_prediction dp
            on fcst.item_id = dp.item_id
            and fcst.sub_id = dp.sub_id
            and fcst.store_code = dp.store_code
            and fcst.date_key > dp.first_delivery_date
            and to_date(fcst.date_key, 'yyyyMMdd') < to_date(dp.theme_start_date, 'yyyy-MM-dd')
    """.replace("\n", " ")

dm_regular_sales = sqlc.sql(dm_regular_sales_sql)

agg_dm_regular_sales = dm_regular_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
    agg(F.sum("sales_prediction").alias("regular_sales_before_dm"))

dm_with_regular = dm_with_orders.join(agg_dm_regular_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

# # For ppp <= 90% npp, get 4 weeks after sales for ROTATION A items

after_fourweek_sql = \
    """
        select 
            dp.item_id ,
            dp.sub_id,
            dp.store_code, 
            dp.dm_theme_id,
            fcst.daily_sales_prediction as sales_prediction
        FROM 
            dm_prediction dp
        JOIN vartefact.v_forecast_daily_sales_prediction fcst
            on fcst.item_id = dp.item_id
            and fcst.sub_id = dp.sub_id
            and fcst.store_code = dp.store_code
            and to_date(fcst.date_key, 'yyyyMMdd') > to_date(dp.theme_end_date, 'yyyy-MM-dd')
            and to_date(fcst.date_key, 'yyyyMMdd') < date_add(to_date(dp.theme_end_date, 'yyyy-MM-dd'), 28)
        WHERE
            dp.rotation ='A'
             AND dp.ppp <= dp.npp * 0.9
    """.replace("\n", " ")

after_fourweek_sales = sqlc.sql(after_fourweek_sql.format(run_date.strftime("%Y%m%d")))

agg_after_fourweek_sales = after_fourweek_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
    agg(F.sum("sales_prediction").alias("four_weeks_after_dm"))

dm_with_fourweek = dm_with_regular.join(agg_after_fourweek_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'],
                                        "left")

# # Fill NA

dm_with_fourweek = dm_with_fourweek.na.fill(0)

# # Final calculation

dm_final = dm_with_fourweek.withColumn("dm_order_qty", dm_with_fourweek.sales_before_order
                                       + dm_with_fourweek.regular_sales_before_dm
                                       + dm_with_fourweek.four_weeks_after_dm
                                       + dm_with_fourweek.dm_sales
                                       - dm_with_fourweek.order_received
                                       - dm_with_fourweek.current_store_stock)

dm_final_pcb = dm_final \
    .withColumn("dm_order_qty_with_pcb", \
                F.when(dm_final.dm_order_qty > 0.0,
                       F.ceil(dm_final.dm_order_qty / dm_final.pcb) * dm_final.pcb).otherwise(int(0)))

print("Number of final result", dm_final_pcb.count())

dm_final_pcb.show()

dm_final_pcb.createOrReplaceTempView("dm_final_pcb")

# +
dm_sql = \
    """
    INSERT INTO vartefact.forecast_dm_orders partition(dm_theme_id)
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
        dm_order_qty_with_pcb,
        dm_theme_id
    FROM dm_final_pcb
    """.replace("\n", " ")

sqlc.sql(dm_sql)
# -

sqlc.sql("refresh table vartefact.forecast_dm_orders")

sc.stop()


