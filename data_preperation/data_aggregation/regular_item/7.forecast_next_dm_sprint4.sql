/*
Description: Create table "{database}.forecast_next_dm_sprint4",
             select the nearest dm_theme_id for a transation

Input:  {database}.forecast_add_future_dms_sprint4
Output: {database}.forecast_next_dm_sprint4
Created: 2019-07-01
*/
-- drop table if exists {database}.forecast_next_dm_sprint4;

CREATE table {database}.forecast_next_dm_sprint4 AS
SELECT
    table0.item_id,
    table0.sub_id,
    table0.city_code,
    table0.trxn_date,
    table0.date_key,
    to_timestamp(table0.theme_start_date, 'yyyy-MM-dd') as next_dm_start_date,
    table0.dm_theme_id as next_dm_theme_id
FROM (
      SELECT
          *,
          ROW_NUMBER() over (PARTITION by item_id, sub_id, city_code, date_key order by theme_start_date asc) as row_num
      from {database}.forecast_add_future_dms_sprint4
) table0
where table0.row_num = 1
;

-- INVALIDATE METADATA {database}.forecast_next_dm_sprint4;
