
-- create view

create view if not exists {database}.forecast_daily_dm_view as
with 
    lastest_week_insert as (
        select
            item_id,
            sub_id,
            store_code,
            dm_theme_id,
            date_key,
            max(insert_date_key) as max_insert_date_key
        from {database}.forecast_DM_results_to_day_all
        group by item_id, sub_id, store_code, dm_theme_id, date_key
    )
select
    lastest.item_id,
    lastest.sub_id,
    all_p.sub_family_code,
    lastest.store_code,
    all_p.prediction,
    all_p.prediction_max,
    all_p.order_prediction,
    lastest.dm_theme_id,
    all_p.theme_start_date,
    all_p.theme_end_date,
    all_p.duration,
    all_p.theme_start_dayofweek,
    all_p.theme_end_dayofweek,
    all_p.dm_pattern_code , 
    all_p.day_of_week,
    all_p.day_number,
    all_p.dm_day_percentage,
    all_p.weekday_subfamily_sales_percentage,
    all_p.weekday_subfamily_percentage_sum,
    all_p.normalized_weekday_percentage,
    all_p.pattern_dm_daily_sales,
    all_p.pattern_dm_daily_sales_original,
    all_p.pattern_dm_order_pred,
    all_p.no_pattern_dm_daily_sales,
    all_p.no_pattern_dm_daily_sales_original,
    all_p.no_pattern_dm_order_pred,
    all_p.no_subfamily_dm_daily_sales,
    all_p.no_subfamily_dm_daily_sales_original,
    all_p.no_subfamily_dm_order_pred,
    all_p.dm_to_daily_pred,
    all_p.dm_to_daily_pred_original,
    all_p.dm_to_daily_order_pred,
    lastest.date_key,
    lastest.max_insert_date_key as lastest_insert_date_key
from lastest_week_insert lastest
left join {database}.forecast_DM_results_to_day_all all_p
    on lastest.item_id = all_p.item_id
    and lastest.sub_id = all_p.sub_id
    and lastest.store_code = all_p.store_code
    and lastest.dm_theme_id = all_p.dm_theme_id
    and lastest.date_key = all_p.date_key
    and lastest.max_insert_date_key = all_p.insert_date_key
    