

CREATE TABLE if not exists {database}.forecast_dm_results_to_day_all
(
    item_id INT,
    sub_id INT,
    sub_family_code STRING,
    store_code STRING,
    prediction DOUBLE,
    prediction_max DOUBLE,
    order_prediction DOUBLE,
    dm_theme_id INT,
    theme_start_date STRING,
    theme_end_date STRING,
    duration INT,
    theme_start_dayofweek INT,
    theme_end_dayofweek INT,
    dm_pattern_code STRING,  
    day_of_week INT,
    day_number INT,
    dm_day_percentage DECIMAL(38,6),
    weekday_subfamily_sales_percentage DECIMAL(38,6),
    weekday_subfamily_percentage_sum DECIMAL(38,6),
    normalized_weekday_percentage DECIMAL(38,6),
    pattern_dm_daily_sales DOUBLE,
    pattern_dm_daily_sales_original DOUBLE,
    pattern_dm_order_pred DOUBLE,
    no_pattern_dm_daily_sales DOUBLE,
    no_pattern_dm_daily_sales_original DOUBLE,
    no_pattern_dm_order_pred DOUBLE,
    no_subfamily_dm_daily_sales DOUBLE,
    no_subfamily_dm_daily_sales_original DOUBLE,
    no_subfamily_dm_order_pred DOUBLE,
    dm_to_daily_pred DOUBLE,
    dm_to_daily_pred_original DOUBLE,
    dm_to_daily_order_pred DOUBLE 
) 
partitioned by (
date_key STRING,
insert_date_key INT
)
STORED AS PARQUET 
