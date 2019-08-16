
-- insert into the table 
insert into table temp.result_forecast_10w_on_the_fututre_all 
partition (
    week_key,
    insert_date_key)
select
    full_item ,
    store_code ,
    train_mape_score ,
    sales_prediction ,
    predict_sales_error_squared ,
    max_confidence_interval ,
    order_prediction ,
    item_id ,
    sub_id ,
    week_key ,
    cast(from_timestamp(now(), 'yyyyMMdd') as int) as insert_date_key  
from temp.result_forecast_10w_on_the_fututre
;