
-- create view

create view if not exists {database}.forecast_daily_normal_view as
with 
    lastest_week_insert as (
        select
            item_id,
            sub_id,
            store_code,
            date_key,
            max(insert_date_key) as max_insert_date_key
        from {database}.forecast_regular_results_week_to_day_original_pred_all
        group by item_id, sub_id, store_code, date_key
    )
select
    lastest.item_id ,
    lastest.sub_id ,
    lastest.store_code ,
    all_p.week_key ,
    all_p.prediction_max ,
    all_p.prediction ,
    all_p.order_prediction ,
    all_p.weekday_percentage ,
    all_p.impacgt ,
    all_p.impacted_weekday_percentage ,
    all_p.daily_sales_pred ,
    all_p.daily_sales_pred_original ,
    all_p.daily_order_pred ,
    lastest.date_key,
    lastest.max_insert_date_key as lastest_insert_date_key
from lastest_week_insert lastest
left join {database}.forecast_regular_results_week_to_day_original_pred_all all_p
    on lastest.item_id = all_p.item_id
    and lastest.sub_id = all_p.sub_id
    and lastest.store_code = all_p.store_code
    and lastest.date_key = all_p.date_key
    and lastest.max_insert_date_key = all_p.insert_date_key
    
