/*
Input: 
    {database}.dm_week_to_day_intermediate
    {database}.dm_pattern_percentage
    {database}.subfamily_store_weekday_percentage
    
Output: {database}.forecast_DM_results_to_day
*/


-- 4. 
-- drop view {database}.forecast_DM_results_to_day;

create view {database}.forecast_DM_results_to_day as
with add_date_key as (
select
    *
from {database}.dm_week_to_day_intermediate
),

add_dm_pattern_percentage as (
select
    a.*,
    b.dm_day_percentage

from add_date_key a
left join {database}.dm_pattern_percentage b
on a.dm_pattern_code = b.dm_pattern_code
and a.day_number = b.day_number
and a.sub_family_code = b.sub_family_code
and a.store_code = b.store_code
),

add_weekday_percentage as (
select
    a.*,
    b.weekday_subfamily_sales_percentage

from add_dm_pattern_percentage a
left join {database}.subfamily_store_weekday_percentage b
on b.store_code = a.store_code
and b.sub_family_code = a.sub_family_code
and b.day_of_week = a.day_of_week
),

dayofweek_impact as (
select
    item_id,
    sub_id,
    sub_family_code,
    store_code,
    dm_theme_id,
    sum(weekday_subfamily_sales_percentage)  as weekday_subfamily_percentage_sum

from add_weekday_percentage
group by
    item_id,
    sub_id,
    sub_family_code,
    store_code,
    dm_theme_id
),

add_dayofweek_impact as (
select
    a.*,
    b.weekday_subfamily_percentage_sum,

    if( b.weekday_subfamily_percentage_sum is not null,
        if(b.weekday_subfamily_percentage_sum <> 0,
            a.weekday_subfamily_sales_percentage / b.weekday_subfamily_percentage_sum,
            null), NUll) as normalized_weekday_percentage

from add_weekday_percentage a
left join dayofweek_impact b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.sub_family_code = a.sub_family_code
and b.store_code = a.store_code
and b.dm_theme_id = a.dm_theme_id
),

combine_percentage as (
select
    *,
    if(dm_day_percentage is not null, prediction_max * dm_day_percentage, 0) as pattern_dm_daily_sales,
    if(dm_day_percentage is not null, prediction * dm_day_percentage, 0) as pattern_dm_daily_sales_original,
    if(dm_day_percentage is not null, order_prediction * dm_day_percentage, 0) as pattern_dm_order_pred,


    if( dm_day_percentage is null,
        if(normalized_weekday_percentage is not null,
             prediction_max * normalized_weekday_percentage, 0),
        0) as no_pattern_dm_daily_sales,
    if( dm_day_percentage is null,
        if(normalized_weekday_percentage is not null,
             prediction * normalized_weekday_percentage, 0),
        0)  as no_pattern_dm_daily_sales_original,
    if( dm_day_percentage is null,
        if(normalized_weekday_percentage is not null,
             order_prediction * normalized_weekday_percentage, 0),
        0)  as no_pattern_dm_order_pred,

    if( dm_day_percentage is null,
        if (normalized_weekday_percentage is null,
            prediction_max/(duration + 1), 0),
        0) as no_subfamily_dm_daily_sales,
    if( dm_day_percentage is null,
        if (normalized_weekday_percentage is null,
            prediction/(duration + 1), 0),
        0) as no_subfamily_dm_daily_sales_original,
    if( dm_day_percentage is null,
        if (normalized_weekday_percentage is null,
            order_prediction/(duration + 1), 0),
        0) as no_subfamily_dm_order_pred

from add_dayofweek_impact
),

dm_to_day as (
select
  *,
  if(pattern_dm_daily_sales <> 0,
     pattern_dm_daily_sales,
     if (no_pattern_dm_daily_sales <>0,
        no_pattern_dm_daily_sales,
        no_subfamily_dm_daily_sales)) as dm_to_daily_pred,

    if(pattern_dm_daily_sales_original <> 0,
         pattern_dm_daily_sales_original,
         if (no_pattern_dm_daily_sales_original <>0,
            no_pattern_dm_daily_sales_original,
            no_subfamily_dm_daily_sales_original)) as dm_to_daily_pred_original,

    if(pattern_dm_order_pred <> 0,
         pattern_dm_order_pred,
         if (no_pattern_dm_order_pred <>0,
            no_pattern_dm_order_pred,
            no_subfamily_dm_order_pred)) as dm_to_daily_order_pred

from combine_percentage
)
select
    *
from dm_to_day
;
