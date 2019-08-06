/*
Input:
    ods.dim_calendar
    ods.nsa_dm_theme
    {database}.DM_pred_results_simple
Output: 
    {database}.dm_week_to_day_intermediate
*/

create view {database}.dm_week_to_day_intermediate as
-- 2158175 row(s)
with calendar as (
select
    date_key
from ods.dim_calendar
where date_key >= '{starting_date}'
-- and date_key < '{ending_date}'
group by
    date_key
),

dm_info as (
select
    dm_theme_id,
    theme_start_date,
    theme_end_date,
    datediff(to_timestamp(theme_end_date, 'yyyy-MM-dd'),
            to_timestamp(theme_start_date, 'yyyy-MM-dd')) as duration,
    dayofweek(to_timestamp(theme_start_date, 'yyyy-MM-dd')) as theme_start_dayofweek,
    dayofweek(to_timestamp(theme_end_date, 'yyyy-MM-dd')) as theme_end_dayofweek

from ods.nsa_dm_theme
where theme_status <> '-1'
and effective_year > cast(substring('{starting_date}', 1,4) as int)
group by
    dm_theme_id,
    theme_start_date,
    theme_end_date
),

add_dm_info_to_pred as (
SELECT
    a.*,
    b.dm_theme_id,
    b.theme_start_date,
    b.theme_end_date,
    b.duration,
    b.theme_start_dayofweek,
    b.theme_end_dayofweek

from {database}.DM_pred_results_simple a
left join dm_info b
on cast(a.current_dm_theme_id as int) = b.dm_theme_id
),

predictions as (
select
    cast(item_id as int) as item_id,
    cast(sub_id as int) as sub_id,
    sub_family_code,
    store_code,
    cast(sales_prediction as double) as prediction,
    cast(max_confidence_interval as double) as prediction_max,
    cast(order_prediction as double) as order_prediction,
    dm_theme_id,
    theme_start_date,
    theme_end_date,
    duration,
    theme_start_dayofweek,
    theme_end_dayofweek

from add_dm_info_to_pred
),

add_dm_pattern as (
select
    a.*,
    b.dm_pattern_code

from predictions a
left join {database}.forecast_dm_pattern b
on cast(b.dm_duration as int) = a.duration
and cast(b.theme_start_dayofweek as int) = a.theme_start_dayofweek
and cast(b.theme_end_dayofweek as int) = a.theme_end_dayofweek
),

add_date_key as (
select
    a.*,
    b.date_key,
    dayofweek(to_timestamp(b.date_key, 'yyyyMMdd')) as day_of_week,
    datediff(to_timestamp(b.date_key, 'yyyyMMdd'),
            to_timestamp(a.theme_start_date, 'yyyy-MM-dd')) as day_number

from add_dm_pattern a
left join calendar b
on to_timestamp(b.date_key, 'yyyyMMdd') >= to_timestamp(a.theme_start_date, 'yyyy-MM-dd')
and to_timestamp(b.date_key, 'yyyyMMdd') <= to_timestamp(a.theme_end_date, 'yyyy-MM-dd')
)

select
    *
from add_date_key
;



-- invalidate metadata {database}.dm_week_to_day_intermediate;

-- drop table {database}.dm_week_to_day_intermediate_parquet;
-- create table {database}.dm_week_to_day_intermediate_parquet LIKE {database}.dm_week_to_day_intermediate
-- STORED AS PARQUET;
-- insert into {database}.dm_week_to_day_intermediate_parquet
-- select *
-- from {database}.dm_week_to_day_intermediate;
-- invalidate metadata {database}.dm_week_to_day_intermediate_parquet;