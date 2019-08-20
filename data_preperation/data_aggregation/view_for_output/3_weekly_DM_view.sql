
-- create view

create view if not exists {database}.forecast_weekly_dm_view as
with 
    lastest_week_insert as (
        select
            item_store,
            item_id,
            sub_id,
            store_code,
            current_dm_theme_id,
            max(insert_date_key) as max_insert_date_key
        from {database}.promo_sales_order_prediction_by_item_store_dm_all
        group by item_store, item_id, sub_id, store_code, current_dm_theme_id
    )
select
    lastest.item_store ,
    lastest.item_id ,
    lastest.sub_id ,
    all_p.sub_family_code ,
    lastest.store_code ,
    lastest.current_dm_theme_id ,
    all_p.sales ,
    all_p.sales_prediction ,
    all_p.squared_error_predicted ,
    all_p.std_dev_predicted ,
    all_p.confidence_interval_max ,
    all_p.order_prediction ,
    lastest.max_insert_date_key as lastest_insert_date_key
from lastest_week_insert lastest
left join {database}.promo_sales_order_prediction_by_item_store_dm_all all_p
    on lastest.item_store = all_p.item_store
    and lastest.item_id = all_p.item_id
    and lastest.sub_id = all_p.sub_id
    and lastest.store_code = all_p.store_code
    and lastest.current_dm_theme_id = all_p.current_dm_theme_id
    and lastest.max_insert_date_key = all_p.insert_date_key

    