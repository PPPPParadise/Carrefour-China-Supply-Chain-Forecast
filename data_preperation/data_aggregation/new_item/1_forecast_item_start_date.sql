/*
Input: 
    ods.p4cm_store_item
    {database}.forecast_store_code_scope_sprint4
    {database}.forecast_sprint4_add_dm_to_daily
Output: {database}.forecast_item_start_date
*/

-- drop table if exists {database}.forecast_item_start_date;
-- create table {database}.forecast_item_start_date as 
-- select 
--     sub_key, 
--     store_code, 
--     min(date_key) as start_date
-- from {database}.forecast_assortment
-- group by sub_key, store_code
-- ;

create table {database}.forecast_item_start_date as 
with forecast_assortment as (
    select *
    from ods.p4cm_store_item
    where date_key >= "{starting_date}"
    and date_key < "{ending_date}"
    and store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
    and sub_id in (select distinct sub_id from {database}.forecast_sprint4_add_dm_to_daily)
)
select 
    sub_id, 
    store_code, 
    min(date_key) as start_date
from forecast_assortment
group by sub_id, store_code
;

