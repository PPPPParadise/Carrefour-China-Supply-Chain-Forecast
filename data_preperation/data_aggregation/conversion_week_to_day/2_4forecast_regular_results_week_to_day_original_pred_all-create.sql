
CREATE TABLE if NOT EXISTS {database}.forecast_regular_results_week_to_day_original_pred_all
(
    item_id INT,
    sub_id INT,
    store_code STRING,
    week_key STRING,
    prediction_max DOUBLE,
    prediction DOUBLE,
    order_prediction DOUBLE,
    weekday_percentage DECIMAL(38,6),
    impacgt DECIMAL(38,6),
    impacted_weekday_percentage DECIMAL(38,6),
    daily_sales_pred DOUBLE,
    daily_sales_pred_original DOUBLE,
    daily_order_pred DOUBLE 
) 
partitioned by (
date_key STRING,
insert_date_key INT
)
STORED AS PARQUET
