
-- insert into the table 

insert OVERWRITE table {database}.promo_sales_order_prediction_by_item_store_dm_all 
partition (
    insert_date_key)
select
    item_store ,
    item_id ,
    sub_id ,
    sub_family_code ,
    store_code ,
    current_dm_theme_id ,
    sales ,
    sales_prediction ,
    squared_error_predicted ,
    std_dev_predicted ,
    confidence_interval_max ,
    order_prediction ,
    cast(from_timestamp(now(), 'yyyyMMdd') as int) as insert_date_key  
from {database}.promo_sales_order_prediction_by_item_store_dm
;