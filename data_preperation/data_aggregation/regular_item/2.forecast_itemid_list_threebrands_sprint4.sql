/*
Description: Create table "{database}.forecast_itemid_list_threebrands_sprint4",
             listing all the item_ids that belong to the three brands.

Input:  {database}.forecast_store_code_scope_sprint4
        nsa.daily_corresponding

Output: {database}.forecast_itemid_list_threebrands_sprint4

Created: 2019-07-01
*/
-- drop table if exists {database}.forecast_itemid_list_threebrands_sprint4;

CREATE TABLE {database}.forecast_itemid_list_threebrands_sprint4 AS
WITH store_code_list AS (
    SELECT
        stostocd
    FROM {database}.forecast_store_code_scope_sprint4
),
item_id_list_PG AS (
    SELECT
        DISTINCT(item_id),
        holding_code
    FROM nsa.daily_corresponding
    WHERE holding_code = '693'
    AND store_code IN (SELECT stostocd FROM store_code_list)
    AND dept_code IN ('11', '12')
    AND date_key >= cast({starting_date} as string)
    AND date_key < cast({ending_date} as string)
),
item_id_list_Unilever AS (
    SELECT
        DISTINCT(item_id),
        holding_code
    FROM nsa.daily_corresponding
    WHERE holding_code = '700'
    AND store_code IN (SELECT stostocd FROM store_code_list)
    AND dept_code IN ('11', '12', '14')
    AND date_key >= cast({starting_date} as string)
    AND date_key < cast({ending_date} as string)
),
item_id_list_Nestle AS (
    SELECT
        DISTINCT(item_id),
        holding_code
    FROM nsa.daily_corresponding
    WHERE holding_code = '002'
    AND store_code IN (SELECT stostocd FROM store_code_list)
    AND dept_code IN ('14', '15')
   AND date_key >= cast({starting_date} as string)
    AND date_key < cast({ending_date} as string)
)
SELECT
    *
FROM (
    SELECT
        *
    FROM item_id_list_PG
    UNION
    SELECT
        *
    FROM item_id_list_Unilever
    UNION
    SELECT
        *
    FROM item_id_list_Nestle
) table0
;

-- INVALIDATE METADATA  {database}.forecast_itemid_list_threebrands_sprint4;
