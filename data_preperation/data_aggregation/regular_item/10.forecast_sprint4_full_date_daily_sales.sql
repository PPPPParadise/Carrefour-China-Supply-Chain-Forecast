/*
Input: 
    {database}.forecast_trxn_flag_v1_sprint4 
    fds.p4cm_daily_stock
    ods.p4cm_store_item 
Output: {database}.forecast_sprint4_full_date_daily_sales
*/
-- drop table if exists {database}.forecast_sprint4_full_date_daily_sales;

create table {database}.forecast_sprint4_full_date_daily_sales as
-- 61139184 row(s)
with stock as (
select
    item_id,
    sub_id,
    dept_code,
    item_code,
    sub_code,
    store_code,
    concat(dept_code,item_code,sub_code) as dept_item_sub_code,
    if(balance_qty <= 0, 1, 0) as out_of_stock_flag,
    date_key,
    stop_month

from fds.p4cm_daily_stock
-- where date_key >= '20170101'
where date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
and store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
and item_id in (select item_id from {database}.forecast_itemid_list_threebrands_sprint4)
),

assort as (
select
    dept_code,
    item_code,
    sub_code,
    store_code,
    date_key,
    sub_id,
    item_stop

from ods.p4cm_store_item
where item_stop = 'N'
and date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
and store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
and concat(dept_code, item_code, sub_code) in (select distinct dept_item_sub_code from stock)
),

add_assort_to_stock as (
select
    a.*,
    b.item_stop

from stock a
left join assort b
on b.dept_code = a.dept_code
and b.item_code = a.item_code
and b.sub_code = a.sub_code
and b.sub_id = a.sub_id
and b.store_code = a.store_code
and b.date_key = a.date_key
),

full_date as (
select
    item_id,
    sub_id,
    store_code,
    date_key,
    out_of_stock_flag,
    item_stop

from add_assort_to_stock
where out_of_stock_flag = 0   -- balance qty > 0
-- or stop_month is null
-- or stop_month = ''
or item_stop = 'N'
group by 
    item_id,
    sub_id,
    store_code,
    date_key,
    out_of_stock_flag,
    item_stop
),

daily_trxn as (
select
    date_key,
    item_id,
    sub_id,
    store_code,
    sum(sales_qty) as sales_qty_daily

from {database}.forecast_trxn_flag_v1_sprint4  
group by
    date_key,
    item_id,
    sub_id,
    store_code
),

full_date_trxn as (
select
    a.*,
    b.sales_qty_daily

from full_date a
left join daily_trxn b
on b.date_key = a.date_key
and b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
),

before_agg as (
select
    item_id,
    sub_id,
    store_code,
    date_key,
    item_stop,
  --  dayofweek(to_timestamp(date_key,'yyyyMMdd')) as day_of_week, 
    out_of_stock_flag,
    if(sales_qty_daily is null, 0, sales_qty_daily) as sales_qty_daily  

from full_date_trxn
)
select
    item_id,
    sub_id,
    store_code,
    date_key,
    item_stop,
    dayofweek(to_timestamp(date_key,'yyyyMMdd')) as day_of_week,
    out_of_stock_flag,
    sales_qty_daily
from before_agg
group by
    item_id,
    sub_id,
    store_code,
    date_key,
    item_stop,
    out_of_stock_flag,
    sales_qty_daily
;

-- invalidate metadata {database}.forecast_sprint4_full_date_daily_sales;