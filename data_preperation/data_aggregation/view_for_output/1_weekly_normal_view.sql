
-- create view

create view if not exists {database}.forecast_weekly_normal_view as
with 
    lastest_week_insert as (
        select
            full_item,
            store_code,
            week_key,
            max(insert_date_key) as max_insert_date_key
        from {database}.result_forecast_10w_on_the_fututre_all
        group by full_item, store_code, week_key
    )
select
    lastest.full_item ,
    lastest.store_code ,
    all_p.train_mape_score ,
    all_p.sales_prediction ,
    all_p.predict_sales_error_squared ,
    all_p.max_confidence_interval ,
    all_p.order_prediction ,
    all_p.item_id ,
    all_p.sub_id ,
    lastest.week_key,
    lastest.max_insert_date_key as lastest_insert_date_key
from lastest_week_insert lastest
left join {database}.result_forecast_10w_on_the_fututre_all all_p
    on lastest.full_item = all_p.full_item
    and lastest.store_code = all_p.store_code
    and lastest.week_key = all_p.week_key
    and lastest.max_insert_date_key = all_p.insert_date_key
