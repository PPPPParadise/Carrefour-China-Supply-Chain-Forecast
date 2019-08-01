/*
Input: 
    ods.nsa_dm_theme 
    {database}.forecast_sprint4_add_dm_to_daily 
    {database}.forecast_dm_pattern
Output:
    {database}.dm_daily_sales
*/ 

CREATE VIEW {database}.dm_daily_sales AS 
WITH dm_info AS (
SELECT 
    dm_theme_id, 
    theme_start_date, 
    theme_end_date 

FROM ods.nsa_dm_theme 
WHERE theme_status != '-1' 
AND effective_year > cast(substring('{starting_date}', 1, 4) as int)
GROUP BY 
    dm_theme_id, 
    theme_start_date, 
    theme_end_date
),
daily_sales AS (
SELECT 
    item_id, 
    sub_id, 
    concat(dept_code, substr(item_code, 1, 3)) sub_family_code, 
    store_code, 
    date_key, 
    daily_sales_sum, 
    current_dm_theme_id 

FROM {database}.forecast_sprint4_add_dm_to_daily 
-- WHERE store_code = '123' 
-- AND 
WHERE date_key >= '{starting_date}' 
AND date_key < '{ending_date}' 
AND current_dm_theme_id IS NOT NULL
),
add_dm_info AS (
SELECT 
    a.*, 
    b.theme_start_date, 
    b.theme_end_date, 
    datediff(to_timestamp(b.theme_end_date, 'yyyy-MM-dd'), 
            to_timestamp(b.theme_start_date, 'yyyy-MM-dd')) duration, 
    dayofweek(to_timestamp(b.theme_start_date, 'yyyy-MM-dd')) theme_start_dayofweek, 
    dayofweek(to_timestamp(b.theme_end_date, 'yyyy-MM-dd')) theme_end_dayofweek 

FROM daily_sales a 
LEFT OUTER JOIN dm_info b 
ON b.dm_theme_id = a.current_dm_theme_id
) 

SELECT 
    a.*, 
    b.dm_pattern_code 

FROM add_dm_info a 
LEFT OUTER JOIN {database}.forecast_dm_pattern b 
ON CAST(b.theme_start_dayofweek AS INT) = a.theme_start_dayofweek 
AND CAST(b.theme_end_dayofweek AS INT) = a.theme_end_dayofweek 
AND CAST(b.dm_duration AS INT) = a.duration
;