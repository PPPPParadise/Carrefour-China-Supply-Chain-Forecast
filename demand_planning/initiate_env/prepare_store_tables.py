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

import warnings

import pandas as pd

warnings.filterwarnings('ignore')

# +
data_or = pd.read_excel('store.xlsx', 'ordinary', header=0, dtype=str).fillna("")
data_or.columns = ['store_code', 'store_cn_name', 'store_en_name',
                   'dept', 'A-order_day', 'A-delivery_day', 'B-order_day', 'B-delivery_day', 'remarks']

data_or_A = data_or[['store_code', 'store_cn_name', 'store_en_name',
                     'dept', 'A-order_day', 'A-delivery_day', 'remarks']]

data_or_A.columns = ['store_code', 'store_cn_name', 'store_en_name',
                     'dept', 'order_type', 'delivery_type', 'remarks']
data_or_A['class'] = 'A'

data_or_B = data_or[['store_code', 'store_cn_name', 'store_en_name',
                     'dept', 'B-order_day', 'B-delivery_day', 'remarks']]

data_or_B.columns = ['store_code', 'store_cn_name', 'store_en_name',
                     'dept', 'order_type', 'delivery_type', 'remarks']

data_or_B['class'] = 'B'

data_or_f = pd.concat([data_or_A, data_or_B], ignore_index=True)

mapping_or = pd.read_excel('order_delivery_mapping.xlsx', 'ordinary', header=0, dtype=str)

res_or = data_or_f.merge(mapping_or, left_on=['class', 'order_type', 'delivery_type'], \
                         right_on=['class', 'order_type', 'delivery_type'])

res_or["week_shift"] = res_or["week_shift"].astype(int)

# +
store_dp = res_or[['store_code','dept']].drop_duplicates()

store_dp["dept1"] = store_dp["dept"]

store_dp = store_dp.set_index(['store_code','dept'])

store_dp = store_dp.stack().str.split(',', expand=True) \
    .stack().apply(pd.Series).stack() \
    .unstack(level=2).reset_index(-1, drop=True).reset_index()

store_dp.columns =['store_code', 'dept', 'dummy', 'dept_code']

store_dp.dept_code = store_dp.dept_code.str.split(' ', 1, expand=True)

store_dp = store_dp[['store_code', 'dept', 'dept_code']].drop_duplicates()
# -

final_mapping = res_or.merge(store_dp, left_on=['store_code', 'dept'], \
                         right_on=['store_code', 'dept'])

final_mapping.columns

stores_dept = final_mapping[['store_code', 'store_cn_name', 'store_en_name', "remarks", 'dept', 'dept_code']].drop_duplicates()

# +
delivery_time = pd.read_excel('store_delivery_time.xlsx', header=0, dtype=str).fillna("")

delivery_time = delivery_time[["store_code","delivery_time"]]
# -

delivery_time.head()

# +
from load_spark import load_spark
from pyspark.sql import HiveContext
sc = load_spark("prepare_store_tables")

sqlc = HiveContext(sc)
# -

onstock_df = sqlc.createDataFrame(final_mapping)
onstock_df.write.mode("overwrite").saveAsTable("vartefact.onstock_order_delivery_mapping")

stores_dept_df = sqlc.createDataFrame(stores_dept)
stores_dept_df.write.mode("overwrite").saveAsTable("vartefact.forecast_stores_dept")

deliver_time_df = sqlc.createDataFrame(delivery_time)
deliver_time_df.write.mode("overwrite").saveAsTable("vartefact.forecast_stores_delv_time")

sc.stop()


