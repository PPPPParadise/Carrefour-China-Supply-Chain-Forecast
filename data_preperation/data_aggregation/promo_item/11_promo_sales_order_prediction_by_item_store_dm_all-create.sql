
-- create the table 
CREATE TABLE if NOT EXISTS {database}.promo_sales_order_prediction_by_item_store_dm_all
( 
    item_store STRING,
    item_id INT,
    sub_id INT,
    sub_family_code STRING,
    store_code STRING,
    current_dm_theme_id INT,
    sales FLOAT,
    sales_prediction FLOAT,
    squared_error_predicted FLOAT,
    std_dev_predicted FLOAT,
    confidence_interval_max FLOAT,
    order_prediction FLOAT
) 
partitioned by (
insert_date_key INT
)
STORED AS PARQUET 
