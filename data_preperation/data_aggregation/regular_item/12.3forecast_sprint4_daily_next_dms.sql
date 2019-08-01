/*
Input: {database}.forecast_sprint4_daily_future_dms
Output: {database}.forecast_sprint4_daily_next_dm
*/
-- drop table if exists {database}.forecast_sprint4_daily_next_dm ;

create table {database}.forecast_sprint4_daily_next_dm as
  -- 3943225 row(s)
  
with all_future_dms as (
select
    *,
    ROW_NUMBER() over (PARTITION by item_id, sub_id, city_code, date_key order by theme_start_date asc) as row_num
from {database}.forecast_sprint4_daily_future_dms
where dm_theme_id is not null
)
select
    item_id,
    sub_id,
    city_code,
    date_key,
    dm_theme_id as next_dm_theme_id

from all_future_dms
where row_num = 1
and dm_theme_id is not null
;
