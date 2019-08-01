/*
Description: Create table "{database}.forecast_sprint3_v10_flag_sprint4"
             add out of stock information from stock table to the weekly aggregated dataset
Input:  {database}.forecast_sprint3_v9_flag_sprint4
        {database}.forecast_out_of_stock_temp
        
Output: {database}.forecast_sprint3_v10_flag_sprint4 
*/
-- DROP table if exists {database}.forecast_sprint3_v10_flag_sprint4;

CREATE table {database}.forecast_sprint3_v10_flag_sprint4 as
--  10317458 row(s)
SELECT
    a.*,
    b.ind_out_of_stock

from {database}.forecast_sprint3_v9_flag_sprint4  a
left join {database}.forecast_out_of_stock_temp b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
and b.week_key = a.week_key
;


--INVALIDATE METADATA {database}.forecast_sprint3_v10_flag_sprint4;
