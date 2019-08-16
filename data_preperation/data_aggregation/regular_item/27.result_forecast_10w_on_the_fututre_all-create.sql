
-- create the table 
CREATE TABLE if NOT EXISTS {database}.result_forecast_10w_on_the_fututre_all
(   
    full_item STRING,
    store_code STRING,
    train_mape_score FLOAT,
    sales_prediction FLOAT,
    predict_sales_error_squared FLOAT,
    max_confidence_interval FLOAT,
    order_prediction FLOAT,
    item_id INT,
    sub_id INT
) 
partitioned by (
    week_key INT,
    insert_date_key INT
)
STORED AS PARQUET 