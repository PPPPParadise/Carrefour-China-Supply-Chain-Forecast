/*
Input: 
    {database}.forecast_regular_day
Output: 
    {database}.forecast_w2d_good_regular_days
*/
-- 2. Find those weeks which meet requirements: one record per each date 
create view {database}.forecast_w2d_good_regular_days as
with good_weeks as (
  with tb1 as (
      SELECT
          item_id,
          sub_id,
          dept_code,
          item_code,
          sub_code,
          store_code,
          week_key,
          count(DISTINCT day_of_week) as num_of_days_in_week

      from {database}.forecast_regular_day
      GROUP BY
          item_id,
          sub_id,
          dept_code,
          item_code,
          sub_code,
          store_code,
          week_key
  )
  select *
  from tb1
  where  num_of_days_in_week  = 7
)

select
    a.*,
    b.date_key,
    b.daily_sales_sum,
    b.out_of_stock_flag,
    b.day_of_week

from good_weeks a
left join {database}.forecast_regular_day b
on a.item_id = b.item_id
and a.sub_id = b.sub_id
and a.store_code = b.store_code
and a.week_key = b.week_key
;
