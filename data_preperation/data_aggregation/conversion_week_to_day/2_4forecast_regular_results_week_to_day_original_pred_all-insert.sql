/*
Input:
    {database}.result_forecast_10w_on_the_fututre
    ods.dim_calendar 
    {database}.forecast_big_events
    {database}.`2018_big_event_impact`
    {database}.forecast_regular_dayofweek_percentage

Output: {database}.forecast_regular_results_week_to_day_original_pred
*/
-- 4. Split weekly prediction to day for demand palnning 
insert into table {database}.forecast_regular_results_week_to_day_original_pred_all 
partition (
    date_key,
    insert_date_key)
select
    item_id ,
    sub_id ,
    store_code ,
    week_key ,
    prediction_max ,
    prediction ,
    order_prediction ,
    weekday_percentage ,
    impacgt ,
    impacted_weekday_percentage ,
    daily_sales_pred ,
    daily_sales_pred_original ,
    daily_order_pred ,
    date_key,
    cast(from_timestamp(now(), 'yyyyMMdd') as int) as insert_date_key  
from {database}.forecast_regular_results_week_to_day_original_pred
;

