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
import calendar
import datetime
from datetime import timedelta
from load_spark import load_spark
from pyspark.sql import HiveContext

sc = load_spark("generate_calendar_table")

sqlc = HiveContext(sc)

sqlc.sql("drop table if exists vartefact.forecast_calendar")

sqlc.sql("drop table if exists vartefact.forecast_dc_order_delivery_mapping")
# -

# # Calendar

# +
nestle_skip_order_day = [datetime.date(2019, 9, 13),datetime.date(2010, 10, 1),datetime.date(2010, 10, 4)]

pg_skip_order_day = [datetime.date(2010, 10, 3),datetime.date(2010, 10, 7)]

un_skip_order_day = [datetime.date(2019, 9, 13)]

# +
start_day = datetime.date(2017, 1, 1)
end_day = datetime.date(2025, 1, 1)
weekday_names = calendar.weekheader

day_i = start_day
week_id = 1
calendar_list = []
dc_list = []

while day_i < end_day:
    calendar_list.append([week_id, day_i.strftime("%Y%m%d"), day_i.isoformat(), day_i.weekday(),
                          day_i.isoweekday(), day_i.strftime("%a"), day_i.strftime("%A")])
        
    delvier_day = day_i + timedelta(days=2)
    risk_unilever_delvier_day = day_i + timedelta(days=3)
    
    # Tuesday
    if day_i.weekday() == 1:
        if day_i not in nestle_skip_order_day:
            dc_list.append(["002", "Shanghai Nestle products Service Co.,Ltd", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 0, 'N']) 

        if day_i not in un_skip_order_day:
            dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 0, 'N']) 

        
    # Friday    
    if day_i.weekday() == 4:
        # Sunday not receving
        delvier_day = delvier_day + timedelta(days=1)
        risk_unilever_delvier_day = risk_unilever_delvier_day + timedelta(days=1)
        
        if day_i not in nestle_skip_order_day:
            dc_list.append(["002", "Shanghai Nestle products Service Co.,Ltd", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 1, 'N']) 
        if day_i not in un_skip_order_day:
            dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 1, 'N']) 
        if day_i not in un_skip_order_day:
            dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        risk_unilever_delvier_day.strftime("%Y%m%d"), risk_unilever_delvier_day.strftime("%a"), 1, 'Y'])

        
    # Monday
    if day_i.weekday() == 0:
        if day_i not in pg_skip_order_day:
            dc_list.append(["693", "Procter&Gamble (China) Sales Co.,Ltd.", 
                day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 0, 'N']) 
            

    # Thursday    
    if day_i.weekday() == 3:
        if day_i not in pg_skip_order_day:
            dc_list.append(["693", "Procter&Gamble (China) Sales Co.,Ltd.", 
                day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                delvier_day.strftime("%Y%m%d"), delvier_day.strftime("%a"), 0, 'N']) 
            
        # Sunday not receving
        risk_unilever_delvier_day = risk_unilever_delvier_day + timedelta(days=1)
        if day_i not in un_skip_order_day:
            dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                        day_i.strftime("%Y%m%d"), day_i.strftime("%a"), 
                        risk_unilever_delvier_day.strftime("%Y%m%d"), risk_unilever_delvier_day.strftime("%a"), 1, 'Y'])

        
    day_i = day_i + timedelta(days=1)
    
    if day_i.weekday() == 0:
        week_id = week_id + 1
        
extra_order_day = datetime.date(2019, 9, 9)
extra_delivery_day = datetime.date(2019, 9, 11)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'N']) 

extra_order_day = datetime.date(2019, 9, 11)
extra_delivery_day = datetime.date(2019, 9, 13)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'N'])  

extra_order_day = datetime.date(2019, 9, 12)
extra_delivery_day = datetime.date(2019, 9, 16)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'N']) 

extra_order_day = datetime.date(2019, 9, 12)
extra_delivery_day = datetime.date(2019, 9, 16)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'Y']) 

extra_order_day = datetime.date(2019, 9, 16)
extra_delivery_day = datetime.date(2019, 9, 18)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'N']) 

extra_order_day = datetime.date(2019, 9, 18)
extra_delivery_day = datetime.date(2019, 9, 20)
dc_list.append(["700", "Unilever Services (Hefei) Co. Ltd.", 
                extra_order_day.strftime("%Y%m%d"), extra_order_day.strftime("%a"), 
                extra_delivery_day.strftime("%Y%m%d"), extra_delivery_day.strftime("%a"), 0, 'N']) 


df = sqlc.createDataFrame(calendar_list,
                          ["week_index", "date_key", "iso_day", "weekday", "iso_weekday", "weekday_short", "weekday_long"])

df = df.withColumn("iso_weekday", df["iso_weekday"].cast("String"))

df = df.withColumn("weekday", df["weekday"].cast("String"))

df = df.withColumn("week_index", df["week_index"].cast("Int"))

dc_df = sqlc.createDataFrame(dc_list,
                          ["con_holding", "holding_name", "order_date", "order_weekday", 
                           "delivery_date", "delivery_weekday", "week_shift", "risk_item_unilever"])

# -

df.write.mode("overwrite").saveAsTable("vartefact.forecast_calendar")

dc_df.write.mode("overwrite").saveAsTable("vartefact.forecast_dc_order_delivery_mapping")

sc.stop()


