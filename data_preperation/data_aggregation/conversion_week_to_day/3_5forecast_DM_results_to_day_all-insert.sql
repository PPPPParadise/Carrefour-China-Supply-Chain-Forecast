/*
Input: 
    {database}.dm_week_to_day_intermediate
    {database}.dm_pattern_percentage
    {database}.subfamily_store_weekday_percentage
    
Output: {database}.forecast_DM_results_to_day
*/


-- 4. 
-- drop view {database}.forecast_DM_results_to_day;

insert into table {database}.forecast_DM_results_to_day_all 
partition (
    date_key,
    insert_date_key)
select
    item_id ,
    sub_id ,
    sub_family_code ,
    store_code ,
    prediction ,
    prediction_max ,
    order_prediction ,
    dm_theme_id ,
    theme_start_date ,
    theme_end_date ,
    duration ,
    theme_start_dayofweek ,
    theme_end_dayofweek ,
    dm_pattern_code ,  
    day_of_week ,
    day_number ,
    dm_day_percentage ,
    weekday_subfamily_sales_percentage ,
    weekday_subfamily_percentage_sum ,
    normalized_weekday_percentage ,
    pattern_dm_daily_sales ,
    pattern_dm_daily_sales_original ,
    pattern_dm_order_pred ,
    no_pattern_dm_daily_sales ,
    no_pattern_dm_daily_sales_original ,
    no_pattern_dm_order_pred ,
    no_subfamily_dm_daily_sales ,
    no_subfamily_dm_daily_sales_original ,
    no_subfamily_dm_order_pred ,
    dm_to_daily_pred ,
    dm_to_daily_pred_original ,
    dm_to_daily_order_pred ,
    date_key,
    cast(from_timestamp(now(), 'yyyyMMdd') as int) as insert_date_key  
from {database}.forecast_DM_results_to_day
;
