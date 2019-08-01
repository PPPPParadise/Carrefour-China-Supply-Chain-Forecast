/*
Input:  {database}.forecast_sprint4_trxn_to_day
        {database}.forecast_dm_plans_sprint4
        {database}.forecast_sprint4_daily_next_dm
Output: {database}.forecast_sprint4_add_dm_to_daily
*/

-- drop table if exists {database}.forecast_sprint4_add_dm_to_daily;

create table {database}.forecast_sprint4_add_dm_to_daily as
--  61139184 row(s)  
with daily_current_dm_info as (

  with dm_plans_agg as (
    select
        item_id,
        sub_code,
        city_code,
        theme_start_date,
        theme_end_date,
        dm_theme_id
    from {database}.forecast_dm_plans_sprint4
    group by
        item_id,
        sub_code,
        city_code,
        theme_start_date,
        theme_end_date,
        dm_theme_id
    )

    select
        a.*,
        b.dm_theme_id as current_dm_theme_id

    from {database}.forecast_sprint4_trxn_to_day a
    left join dm_plans_agg b     
    on b.item_id = a.item_id
    and b.sub_code = a.sub_code
    and b.city_code = a.city_code
    AND to_date(to_timestamp(a.date_key, 'yyyyMMdd')) >= to_date(b.theme_start_date)
    AND to_date(to_timestamp(a.date_key, 'yyyyMMdd')) <= to_date(b.theme_end_date)
),

daily_next_dm as (
  select
        item_id,
        sub_id,
        city_code,
        date_key,
        next_dm_theme_id
  from  {database}.forecast_sprint4_daily_next_dm   
)

select
    a.*,
    b.next_dm_theme_id

from daily_current_dm_info a
left join daily_next_dm  b
on a.item_id = b.item_id
and a.sub_id = b.sub_id
and a.city_code = b.city_code
and a.date_key = b.date_key
;

-- INVALIDATE METADATA {database}.forecast_sprint4_add_dm_to_daily;


