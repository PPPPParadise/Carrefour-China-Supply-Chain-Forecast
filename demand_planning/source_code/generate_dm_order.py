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
date_str = '20190819'

run_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()

# starting day of the DM calculation period
start_date = run_date + timedelta(weeks=4)

# end day of the DM calculation period
end_date = run_date + timedelta(weeks=5)

stock_date = run_date + timedelta(days=-1)

# +
#item_id = 1056574
item_id = None

# sub_id = 1431061
sub_id = None

# store_code = "150"
store_code = None

dm_theme_id = None

# +
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
        id.store_code,
        del.dept_code,
        id.con_holding,
        id.risk_item_unilever,
        cast(id.qty_per_unit as int) as pcb,
        id.dc_supplier_code,
        id.ds_supplier_code,
        id.rotation,
        icis.item_id,
        icis.sub_id,
        icis.item_code,
        icis.sub_code,
        icis.date_key AS run_date,
        fdo.first_order_date AS past_result
    FROM vartefact.forecast_nsa_dm_extract_log del
    JOIN ods.nsa_dm_theme ndt ON del.dm_theme_id = ndt.dm_theme_id
    JOIN ods.p4md_stogld ps ON del.city_code = ps.stocity
    JOIN vartefact.v_forecast_inscope_store_item_details id ON ps.stostocd = id.store_code
        AND del.item_code = CONCAT (
            id.dept_code,
            id.item_code
            )
        AND del.sub_code = id.sub_code
        AND del.dept_code = id.dept_code
    JOIN vartefact.forecast_item_code_id_stock icis ON icis.date_key = '{0}'
        AND id.item_code = icis.item_code
        AND id.sub_code = icis.sub_code
        AND id.dept_code = icis.dept_code
        AND id.store_code = icis.store_code
    LEFT JOIN vartefact.forecast_simulation_dm_orders fdo ON ndt.dm_theme_id = fdo.dm_theme_id
        AND icis.dept_code = fdo.dept_code
        AND icis.item_code = fdo.item_code
        AND icis.sub_code = fdo.sub_code
        AND icis.store_code = fdo.store_code
    WHERE del.extract_order = 40
        AND ndt.theme_start_date >= '{1}'
        AND ndt.theme_start_date < '{2}'
    """.replace("\n", " ")

dm_item_store_sql = dm_item_store_sql.format(stock_date.strftime("%Y%m%d"), start_date.isoformat(),
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

dm_item_store_df.createOrReplaceTempView("dm_item_store")

# # The first order day within PPP period

# +
onstock_order_sql = \
    """
    SELECT dis.item_id,
        dis.sub_id,
        dis.store_code,
        ord.date_key AS first_order_date,
        dev.date_key AS first_delivery_date
    FROM dm_item_store dis
    JOIN vartefact.forecast_onstock_order_delivery_mapping mp ON dis.dept_code = mp.dept_code
        AND dis.rotation = mp.rotation
        AND dis.store_code = mp.store_code
    JOIN vartefact.forecast_calendar ord ON ord.iso_weekday = mp.order_iso_weekday
    JOIN vartefact.forecast_calendar dev ON dev.iso_weekday = mp.delivery_iso_weekday
        AND dev.week_index = ord.week_index + mp.week_shift
    WHERE to_timestamp(ord.date_key, 'yyyyMMdd') >= to_timestamp(dis.ppp_start_date, 'yyyy-MM-dd')
        AND to_timestamp(dev.date_key, 'yyyyMMdd') >= date_add(to_timestamp(dis.theme_start_date, 'yyyy-MM-dd'), -7)
        AND dev.date_key <= '{0}'
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


# +
xdock_order_sql = \
    """
    SELECT dis.item_id,
        dis.sub_id,
        dis.store_code,
        ord.date_key AS first_order_date,
        date_format(
            date_add(
                to_timestamp(dodm.delivery_date, 'yyyyMMdd'), xo.dc_to_store_time
                ),
            'yyyyMMdd'
        ) AS first_delivery_date
    FROM dm_item_store dis
    JOIN vartefact.forecast_xdock_order_mapping xo ON dis.item_code = xo.item_code
        AND dis.sub_code = xo.sub_code
        AND dis.dept_code = xo.dept_code
        AND dis.store_code = xo.store_code
    JOIN vartefact.forecast_calendar ord ON ord.iso_weekday = xo.order_iso_weekday
    JOIN vartefact.forecast_dc_order_delivery_mapping dodm ON dodm.con_holding = dis.con_holding
        AND dodm.order_date = ord.date_key
        AND dis.risk_item_unilever = dodm.risk_item_unilever
    WHERE to_timestamp(ord.date_key, 'yyyyMMdd') >= to_timestamp(dis.ppp_start_date, 'yyyy-MM-dd')
        AND date_add(to_timestamp(dodm.delivery_date, 'yyyyMMdd'), xo.dc_to_store_time)  <= to_timestamp('{0}', 'yyyyMMdd')
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

# +
order_deliver_df = onstock_order_deliver_df.union(xdock_order_deliver_df)

first_order_df = order_deliver_df.groupBy(['item_id', 'sub_id', 'store_code']). \
        agg(F.min("first_order_date").alias("first_order_date"))
# -

first_order_deliver_df = order_deliver_df \
        .select(['item_id', 'sub_id', 'store_code', 'first_order_date', 'first_delivery_date']) \
        .join(first_order_df, ['item_id', 'sub_id', 'store_code', 'first_order_date'])

dm_item_store_order_df = dm_item_store_df \
    .join(first_order_deliver_df, \
          ['item_id', 'sub_id', 'store_code'])

dm_item_store_order_df.createOrReplaceTempView("dm_item_store_order")

print("Number of item stores that will have DM", dm_item_store_order_df.count())

# # Get DM sales prediction

dm_sales_predict_sql = \
    """
    select 
      dm.*,
      cast(coalesce(pred.sales_prediction, '0', pred.sales_prediction) as double) as dm_sales,
      coalesce(pred.sales_prediction, 'no', 'yes') as having_dm_prediction
    from 
        dm_item_store_order dm
    left join vartefact.forecast_weekly_dm_view pred
        on cast(pred.item_id as int) = dm.item_id
        and cast(pred.sub_id as int) = dm.sub_id
        and cast(pred.current_dm_theme_id as int) = dm.dm_theme_id
        and pred.store_code = dm.store_code
    """.replace("\n", " ")

dm_prediction = sqlc.sql(dm_sales_predict_sql)

dm_prediction.filter("having_dm_prediction = 'no' ").write.mode("overwrite").format("parquet").saveAsTable("vartefact.forecast_no_dm_prediction")

dm_prediction.createOrReplaceTempView("dm_prediction")

print("Number of item stores with DM prediction", dm_prediction.count())

# # Regular sales from first order day to DM start day

dm_regular_sales_sql = \
    """
    SELECT dp.item_id,
        dp.sub_id,
        dp.store_code,
        dp.dm_theme_id,
        case when
        fcst.daily_sales_prediction_original < 0.2
            then 0
        else fcst.daily_sales_prediction_original
        end AS sales_prediction
    FROM vartefact.t_forecast_daily_sales_prediction fcst
    JOIN dm_prediction dp ON fcst.item_id = dp.item_id
        AND fcst.sub_id = dp.sub_id
        AND fcst.store_code = dp.store_code
        AND fcst.date_key > dp.first_delivery_date
        AND to_timestamp(fcst.date_key, 'yyyyMMdd') < to_timestamp(dp.theme_start_date, 'yyyy-MM-dd')
    """.replace("\n", " ")


dm_regular_sales = sqlc.sql(dm_regular_sales_sql)

agg_dm_regular_sales = dm_regular_sales.groupBy(['item_id', 'sub_id', 'store_code', 'dm_theme_id']). \
    agg(F.sum("sales_prediction").alias("regular_sales_before_dm"))

dm_with_regular = dm_prediction.join(agg_dm_regular_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'], "left")

# # For ppp <= 90% npp, get 4 weeks after sales for ROTATION A items

after_fourweek_sql = \
    """
    SELECT dp.item_id,
        dp.sub_id,
        dp.store_code,
        dp.dm_theme_id,
        case when
        fcst.daily_sales_prediction_original < 0.2
            then 0
        else fcst.daily_sales_prediction_original
        end AS sales_prediction
    FROM dm_prediction dp
    JOIN vartefact.t_forecast_daily_sales_prediction fcst ON fcst.item_id = dp.item_id
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

dm_with_fourweek = dm_with_regular.join(agg_after_fourweek_sales, ['item_id', 'sub_id', 'store_code', 'dm_theme_id'],
                                        "left")

# # Fill NA

dm_with_fourweek = dm_with_fourweek.na.fill(0)

# # Final calculation

dm_final = dm_with_fourweek.withColumn("dm_order_qty_without_pcb",
                                       dm_with_fourweek.regular_sales_before_dm
                                       + dm_with_fourweek.four_weeks_after_dm
                                       + dm_with_fourweek.dm_sales)

dm_final = dm_final \
        .withColumn("first_dm_order_qty_without_pcb",
                F.when(dm_final.rotation != 'X', 0.75 * dm_final.dm_order_qty_without_pcb)
                .otherwise(dm_final.dm_order_qty_without_pcb))

dm_final = dm_final \
        .withColumn("first_dm_order_qty",
                    F.when(dm_final.first_dm_order_qty_without_pcb > 0.0,
                           F.ceil(dm_final.first_dm_order_qty_without_pcb / dm_final.pcb) * dm_final.pcb)
                    .otherwise(0))

dm_final_pcb = dm_final \
        .withColumn("dm_order_qty",
                    F.when(dm_final.dm_order_qty_without_pcb > 0.0,
                           F.ceil(dm_final.dm_order_qty_without_pcb / dm_final.pcb) * dm_final.pcb)
                    .otherwise(0))

# +
dm_final_pcb = dm_final_pcb.withColumn("first_dm_order_qty", dm_final_pcb["first_dm_order_qty"].cast("Int"))

dm_final_pcb = dm_final_pcb.withColumn("dm_order_qty", dm_final_pcb["dm_order_qty"].cast("Int"))

# +
#print("Number of final result", dm_final_pcb.count())
# -

dm_final_pcb.createOrReplaceTempView("dm_final_pcb")

dm_final_pcb.write.mode("overwrite").format("parquet").saveAsTable("vartefact.forecast_sep_dm_original")

dm_sql = \
    """
    INSERT INTO vartefact.forecast_simulation_dm_orders
    PARTITION (dm_theme_id)
    SELECT 
        item_id,
        sub_id,
        store_code,
        con_holding,
        theme_start_date,
        theme_end_date,
        npp,
        ppp,
        ppp_start_date,
        ppp_end_date,
        city_code,
        dept_code,
        item_code,
        sub_code,
        pcb,
        dc_supplier_code,
        ds_supplier_code,
        rotation,
        run_date,
        first_order_date,
        first_delivery_date,
        regular_sales_before_dm,
        four_weeks_after_dm,
        dm_sales,
        dm_order_qty,
        first_dm_order_qty,
        dm_order_qty_without_pcb,
        dm_theme_id
    FROM dm_final_pcb
    """.replace("\n", " ")

# +
#sqlc.sql(dm_sql)
# -

sqlc.sql("refresh table vartefact.forecast_simulation_dm_orders")

# # DC Order

# +
dm_item_dc_sql = \
    """
    SELECT distinct ndt.dm_theme_id,
        ndt.theme_start_date,
        ndt.theme_end_date,
        del.npp,
        del.ppp,
        del.ppp_start_date,
        del.ppp_end_date,
        del.dept_code,
        dcid.holding_code,
        dcid.risk_item_unilever,
        dcid.primary_ds_supplier as ds_supplier_code,
        cast(dcid.qty_per_unit as int) as pcb,
        dcid.rotation,
        dcid.qty_per_unit,
        icis.item_id,
        icis.sub_id,
        icis.item_code,
        icis.sub_code,
        icis.date_key AS run_date
    FROM vartefact.forecast_nsa_dm_extract_log del
    JOIN ods.nsa_dm_theme ndt ON del.dm_theme_id = ndt.dm_theme_id
    JOIN vartefact.forecast_item_code_id_stock icis ON icis.date_key = '{0}'
        AND del.item_code = CONCAT (
            icis.dept_code,
            icis.item_code
            )
        AND del.sub_code = icis.sub_code
        AND del.dept_code = icis.dept_code
    JOIN vartefact.v_forecast_inscope_dc_item_details dcid ON dcid.item_code =icis.item_code
        AND dcid.sub_code = icis.sub_code
        AND dcid.dept_code = icis.dept_code
    WHERE del.extract_order = 50
        AND ndt.theme_start_date >= '{1}'
        AND ndt.theme_start_date <= '{2}'
    """.replace("\n", " ")

dm_item_dc_sql = dm_item_dc_sql.format(run_date.strftime("%Y%m%d"), start_date.isoformat(),
                                       end_date.isoformat())
# -

dm_item_dc_df = sqlc.sql(dm_item_dc_sql)

# # First order day for DC

# +
first_dc_dm = dm_item_dc_df. \
        groupBy(['item_id', 'sub_id']). \
        agg(F.min("theme_start_date").alias("theme_start_date"))

dm_item_dc_df = dm_item_dc_df.join(first_dc_dm, ['item_id', 'sub_id', 'theme_start_date'])

dm_item_dc_df.cache()

dm_item_dc_df.createOrReplaceTempView("dm_item_dc")

# +
dc_order_sql = \
        """
        SELECT distinct dis.item_id,
            dis.sub_id,
            ord.date_key AS first_order_date,
            dev.date_key AS first_delivery_date
        FROM dm_item_dc dis
        JOIN vartefact.forecast_dc_order_delivery_mapping dodm
            ON dis.holding_code = dodm.con_holding
            AND dis.risk_item_unilever = dodm.risk_item_unilever
        JOIN vartefact.forecast_calendar ord
            ON ord.date_key = dodm.order_date
        JOIN vartefact.forecast_calendar dev
            ON dev.weekday_short = dodm.delivery_weekday and dev.week_index = ord.week_index + dodm.week_shift
        WHERE to_timestamp(ord.date_key, 'yyyyMMdd') >= to_timestamp(dis.ppp_start_date, 'yyyy-MM-dd')
            AND dev.date_key <= '{0}'
            AND dis.rotation != 'X'
        """.replace("\n", " ")

dc_order_sql = dc_order_sql.format(end_date.strftime("%Y%m%d"))

# +
dc_order_deliver_df = sqlc.sql(dc_order_sql)

dc_first_order_df = dc_order_deliver_df.groupBy(['item_id', 'sub_id']). \
    agg(F.min("first_order_date").alias("first_order_date"))

dc_first_order_deliver_df = dc_order_deliver_df \
    .select(['item_id', 'sub_id', 'first_order_date', 'first_delivery_date']) \
    .join(dc_first_order_df, ['item_id', 'sub_id', 'first_order_date'])

# +
dm_item_dc_order_df = dm_item_dc_df \
        .join(dc_first_order_deliver_df, \
              ['item_id', 'sub_id'])

dm_item_dc_order_df.createOrReplaceTempView("dm_item_dc_order")
# -

dm_store_to_dc_sql = \
    """
    select 
      dm.item_id,
      dm.sub_id,
      dm.holding_code,
      dm.theme_start_date,
      dm.theme_end_date,
      dm.npp,
      dm.ppp,
      dm.ppp_start_date,
      dm.ppp_end_date,
      dm.dept_code,
      dm.item_code,
      dm.sub_code,
      dm.pcb,
      dm.ds_supplier_code,
      dm.rotation,
      dm.run_date,
      dm.first_order_date,
      dm.first_delivery_date,
      sum(sod.regular_sales_before_dm) as regular_sales_before_dm,
      sum(sod.four_weeks_after_dm) as four_weeks_after_dm,
      sum(sod.dm_sales) as dm_sales,
      sum(sod.dm_order_qty) as dm_order_qty_without_pcb,
      dm.dm_theme_id
    FROM 
        vartefact.forecast_sep_dm_original sod
    JOIN dm_item_dc_order dm
        on sod.item_id = dm.item_id
        and sod.sub_id = dm.sub_id
        and sod.dm_theme_id = dm.dm_theme_id
    GROUP BY
      dm.dm_theme_id,
      dm.item_id,
      dm.sub_id,
      dm.holding_code,
      dm.theme_start_date,
      dm.theme_end_date,
      dm.npp,
      dm.ppp,
      dm.ppp_start_date,
      dm.ppp_end_date,
      dm.dept_code,
      dm.item_code,
      dm.sub_code,
      dm.pcb,
      dm.ds_supplier_code,
      dm.rotation,
      dm.run_date,
      dm.first_order_date,
      dm.first_delivery_date
    """.replace("\n", " ")

dm_dc_order = sqlc.sql(dm_store_to_dc_sql)

# +
dm_dc_pcb = dm_dc_order \
        .withColumn("dm_order_qty",
                    F.when(dm_dc_order.dm_order_qty_without_pcb > 0.0,
                           F.ceil(dm_dc_order.dm_order_qty_without_pcb / dm_dc_order.pcb) * dm_dc_order.pcb)
                    .otherwise(int(0)))

dm_dc_pcb.createOrReplaceTempView("dm_dc_final")
# -

dm_dc_pcb.write.mode("overwrite").format("parquet").saveAsTable("vartefact.forecast_sep_dm_dc_original")

dm_dc_sql = \
    """
    INSERT INTO vartefact.forecast_simulation_dm_dc_orders
    PARTITION (dm_theme_id)
    SELECT 
      item_id,
      sub_id,
      holding_code,
      theme_start_date,
      theme_end_date,
      npp,
      ppp,
      ppp_start_date,
      ppp_end_date,
      dept_code,
      item_code,
      sub_code,
      pcb,
      ds_supplier_code,
      rotation,
      run_date,
      first_order_date,
      first_delivery_date,
      regular_sales_before_dm,
      four_weeks_after_dm,
      dm_sales,
      dm_order_qty,
      dm_order_qty_without_pcb,
      dm_theme_id
    FROM dm_dc_final
    """.replace("\n", " ")

# +
#sqlc.sql(dm_dc_sql)
# -

sqlc.sql("refresh table vartefact.forecast_simulation_dm_dc_orders")

sc.stop()


