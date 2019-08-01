/*
Input:  {database}.forecast_sprint4_full_date_daily_sales  
Output: {database}.forecast_sprint4_out_of_stock_median
*/

-- drop table if exists {database}.forecast_sprint4_out_of_stock_median;

create table {database}.forecast_sprint4_out_of_stock_median as
--  3081895 row(s)
with tb1 as (
SELECT
   *
from {database}.forecast_sprint4_full_date_daily_sales
where out_of_stock_flag = 0
or item_stop = 'N'
),

tb2 as (
select
    a.*,
    b.sales_qty_daily as relative_sales_qty_daily

from tb1 a
left join tb1 b
on a.item_id = b.item_id
and a.sub_id = b.sub_id
and a.store_code = b.store_code
and a.day_of_week = b.day_of_week
and datediff(to_timestamp(a.date_key, 'yyyyMMdd'), to_timestamp(b.date_key, 'yyyyMMdd')) between 0 and 90
)

select
    item_id,
    sub_id,
    store_code,
    date_key,
    day_of_week,
    out_of_stock_flag,
    item_stop,
    appx_median(relative_sales_qty_daily) as median_daily_sales_qty

from tb2
where out_of_stock_flag = 1
group by
    item_id,
    sub_id,
    store_code,
    date_key,
    day_of_week,
    out_of_stock_flag,
    item_stop
;
