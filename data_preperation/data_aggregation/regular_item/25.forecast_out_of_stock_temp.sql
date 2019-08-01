/*
Description: Create table "{database}.forecast_out_of_stock_temp"
             Create out of stock flags

Input:  fds.p4cm_daily_stock
        ods.dim_calendar
        {database}.forecast_store_code_scope_sprint4
        {database}.forecast_itemid_list_threebrands_sprint4

Output: {database}.forecast_out_of_stock_temp
*/

-- drop table if exists {database}.forecast_out_of_stock_temp;

create table {database}.forecast_out_of_stock_temp as
with out_stock_day as (
select
    item_id,
    sub_id,
    store_code,
    if( balance_qty <= 0, 1, 0) as out_of_stock_flag ,
    date_key

from fds.p4cm_daily_stock
where date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
and store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
and item_id in (select item_id from {database}.forecast_itemid_list_threebrands_sprint4)
),
date_week as (
select
    date_key,
    week_key
from ods.dim_calendar
where date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
group by
    date_key,
    week_key
),

add_key as (
select
   a.*,
   b.week_key
from out_stock_day a
left join date_week b
on b.date_key = a.date_key
)

select
    item_id,
    sub_id,
    store_code,
    if(sum(out_of_stock_flag) > 0, 1, 0) as ind_out_of_stock,
    week_key

from add_key
group by
    item_id,
    sub_id,
    store_code,
    week_key
;

-- INVALIDATE METADATA {database}.forecast_out_of_stock_temp;
