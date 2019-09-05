# -*- coding: utf-8 -*-
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

import math
import warnings
import pandas as pd

# # Read Excel

# +
warnings.filterwarnings('ignore')

excel_input = pd.read_excel('East Sep 17 DM list.xlsx', 'Sheet1', header=0, dtype=str).fillna("")
# -

# # Get store item list

store_items = excel_input[["Store", "Dept", "Item Code 1", "Sub Code"]].drop_duplicates().reset_index(drop=True)

store_items["item_code"] = store_items["Item Code 1"].str.slice(2, 8)

store_items = store_items.drop(["Item Code 1"], axis=1)

store_items.columns = ["store_code", "dept_code", "sub_code", "item_code"]

# +
from load_spark import load_spark
from pyspark.sql import HiveContext

sc = load_spark("Load September 17 DM store items")

sqlc = HiveContext(sc)

# +
store_items_df = sqlc.createDataFrame(store_items)

store_items_df.write.mode("overwrite").saveAsTable("vartefact.forecast_sep_17_dm_store_items")
# -

sc.stop()


