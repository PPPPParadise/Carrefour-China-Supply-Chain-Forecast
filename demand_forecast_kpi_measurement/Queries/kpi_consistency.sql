/* =======================================================
                      Module KPI Monitor
                        Consistency
==========================================================
*/

/*
Input:
    * lfms.daily_dctrxn
    * lfms.ord
To:
    * vartefact.service_level_item_bc

# Logic
* the business people may change the actual order, need to monitor this
* forecast order should be saved in some table in data lake
*/


drop table if exists vartefact.monitor_consistency;

create table vartefact.monitor_consistency as

with 

-- with flow_type as
-- (
--     select
--         concat(dept_code, item_code, sub_code) as item_code_long,
--         rotation
--     from 
--         vartefact.forecast_item_details
-- ),

/*
forecast_table: run_date, date_key, item, ord_forecast
order_table: date_key, item, order_qty
*/