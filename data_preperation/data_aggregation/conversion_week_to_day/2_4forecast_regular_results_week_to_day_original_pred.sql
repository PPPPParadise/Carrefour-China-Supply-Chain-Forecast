/*
Input:
    {database}.result_forecast_10w_on_the_fututre
    ods.dim_calendar 
    {database}.forecast_big_events
    {database}.`2018_big_event_impact`
    {database}.forecast_regular_dayofweek_percentage

Output: {database}.forecast_regular_results_week_to_day_original_pred
*/
-- 4. Split weekly prediction to day for demand palnning 
CREATE table {database}.forecast_regular_results_week_to_day_original_pred stored as parquet AS 
WITH art_pred AS (
    SELECT 
        CAST(item_id AS INT) item_id, 
        CAST(sub_id AS INT) sub_id, 
        store_code, 
        CAST(week_key AS STRING) week_key,
        CAST(max_confidence_interval AS DOUBLE) prediction_max, 
        CAST(sales_prediction AS DOUBLE) prediction, 
        CAST(order_prediction AS DOUBLE) order_prediction 
    
    FROM {database}.result_forecast_10w_on_the_fututre
),

calendar AS (
    SELECT 
        date_key,
        week_key 
    FROM ods.dim_calendar 
    GROUP BY 
        date_key, 
        week_key
),

pred_add_calendar_event AS (
    SELECT 
        pred.*, 
        cal.date_key, 
        dayofweek(to_timestamp(cal.date_key, 'yyyyMMdd')) day_of_week, 
        event.big_event_code 

    FROM art_pred pred 
    LEFT OUTER JOIN calendar cal 
    ON cal.week_key = pred.week_key 
    
    LEFT OUTER JOIN {database}.forecast_big_events event 
    ON event.date_key = cal.date_key
),
    
add_weekday_sales_percentage AS (
    SELECT 
        pred.*, 
        dow_per.weekday_percentage 
    
    FROM pred_add_calendar_event pred 
    LEFT OUTER JOIN {database}.forecast_regular_dayofweek_percentage dow_per 
    ON dow_per.item_id = pred.item_id 
    AND dow_per.sub_id = pred.sub_id 
    AND dow_per.store_code = pred.store_code 
    AND dow_per.day_of_week = pred.day_of_week
),

big_event_impact AS (
    WITH tb1 AS (
        SELECT 
            a.item_id, 
            a.sub_id, 
            a.store_code, 
            if(a.impacgt IS NULL, 0, a.impacgt) impacgt, 
            b.big_event_code 
        FROM {database}.`2018_big_event_impact` a 
        LEFT OUTER JOIN {database}.forecast_big_events b 
        ON b.date_key = a.date_key
        ) 
    SELECT 
        * 
    FROM tb1 
    WHERE big_event_code IS NOT NULL
),

add_event_impact AS (
    SELECT 
        a.*, 
        if(b.impacgt IS NULL, 0, b.impacgt) impacgt 

    FROM add_weekday_sales_percentage a 
    LEFT OUTER JOIN big_event_impact b 
    ON b.item_id = a.item_id 
    AND b.sub_id = a.sub_id 
    AND b.store_code = a.store_code 
    AND b.big_event_code = a.big_event_code
),

impacted_percentage AS (
    SELECT 
        item_id, 
        sub_id, 
        store_code, 
        week_key, 
        prediction_max, 
        prediction, 
        order_prediction, 
        date_key, 
        weekday_percentage, 
        impacgt, 
        (1 + impacgt) * weekday_percentage impacted_weekday_percentage 
    
    FROM add_event_impact
),

rebase AS (
    SELECT 
        item_id, 
        sub_id, 
        store_code, 
        week_key, 
        sum(impacted_weekday_percentage) impacted_weekday_percentage_week 

    FROM impacted_percentage 
    GROUP BY 
        item_id, 
        sub_id, 
        store_code, 
        week_key
) 
    
SELECT 
    a.*, 
    CAST(if(b.impacted_weekday_percentage_week is not null, 
            if(b.impacted_weekday_percentage_week != 0, a.impacted_weekday_percentage * a.prediction_max / b.impacted_weekday_percentage_week, a.prediction_max/7),
            a.prediction_max/7) AS DOUBLE) daily_sales_pred, 

    CAST(if(b.impacted_weekday_percentage_week is not null, 
        if(b.impacted_weekday_percentage_week != 0, a.impacted_weekday_percentage * a.prediction / b.impacted_weekday_percentage_week, a.prediction/7),
        a.prediction/7) AS DOUBLE) daily_sales_pred_original, 

    CAST(if(b.impacted_weekday_percentage_week is not null, 
        if(b.impacted_weekday_percentage_week != 0, a.impacted_weekday_percentage * a.order_prediction / b.impacted_weekday_percentage_week, a.order_prediction/7),
        a.order_prediction/7) AS DOUBLE) daily_order_pred    

FROM impacted_percentage a 
LEFT OUTER JOIN rebase b 
ON b.item_id = a.item_id 
AND b.sub_id = a.sub_id 
AND b.store_code = a.store_code 
AND b.week_key = a.week_key
;

