/*
Input:  {database}.forecast_sprint4_trxn_to_day
        {database}.forecast_dm_plans_sprint4
Output: {database}.forecast_sprint4_daily_future_dms
*/

-- drop table if exists {database}.forecast_sprint4_daily_future_dms;

create table {database}.forecast_sprint4_daily_future_dms as
-- 160039313 row(s)
with daily_store as (
  select
      item_id,
      sub_id,
      sub_code,
      store_code,
      date_key,
      city_code

  from {database}.forecast_sprint4_trxn_to_day  
),

future_dm_plan as (
    select
        item_id,
        sub_id,
        sub_code,
        city_code,
        theme_start_date,
        dm_theme_id
    from {database}.forecast_dm_plans_sprint4
    -- where extract_order = 50
)

select
  a.*,
  b.dm_theme_id,
  b.theme_start_date

from daily_store a
left join future_dm_plan b
on b.item_id = a.item_id
and b.sub_code = a.sub_code
and b.city_code = a.city_code
and to_date(to_timestamp(a.date_key, 'yyyyMMdd')) < to_date(b.theme_start_date)
;
