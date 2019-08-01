/*
Description: Create table "{database}.forecast_trxn_v7_full_item_id_sprint4",
             Add those item ids that dosen't have any transaction during the past 3 years

Input:  {database}.forecast_itemid_list_threebrands_sprint4
        {database}.forecast_trxn_v7_sprint4

Output: {database}.forecast_trxn_v7_full_item_id_sprint4
*/

-- drop table if exists {database}.forecast_trxn_v7_full_item_id_sprint4;

create table {database}.forecast_trxn_v7_full_item_id_sprint4 as
WITH three_brand_item_list AS (
    SELECT
        *
    FROM {database}.forecast_itemid_list_threebrands_sprint4
)

select
    a.item_id as three_brand_item_list_id,
    a.holding_code as three_brand_item_list_holding_code,
    b.*

from three_brand_item_list a
left join {database}.forecast_trxn_v7_sprint4 b
on b.item_id = a.item_id;

-- INVALIDATE METADATA {database}.forecast_trxn_v7_full_item_id_sprint4;
