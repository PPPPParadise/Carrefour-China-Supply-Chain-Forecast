/*
Description: Create table "{database}.forecast_assortment"
             Since the item_ids are missing from assortment table [ods.p4cm_store_item],
             [fds.p4cm_item_map] has to be used to add item_id back
Input:  ods.p4cm_store_item
        {database}.p4cm_item_map_complete  
        {database}.forecast_store_code_scope_sprint4
        {database}.forecast_itemid_list_threebrands_sprint4

Output: {database}.forecast_assortment_full
*/

-- drop table if exists {database}.forecast_assortment_full;

create table {database}.forecast_assortment_full as
-- 864415547 row(s)
with forecast_assortment as (
SELECT date_key,
       store_code,
       dept_code,
       item_code,
       sub_code,
       unit_code,
       bar_code,
       -- item_id,  this item_id cannot be used
       -- sub_key,
       sub_id,
       ms_code,
       item_stop,
       item_stop_reason,
       item_stop_start_date,
       item_stop_end_date,
       sub_stop,
       sub_stop_reason,
       sub_stop_start_date,
       sub_stop_end_date,
       nsp,
       psp,
       psp_start_date,
       psp_end_date

FROM ods.p4cm_store_item
where date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
and store_code in (SELECT stostocd from {database}.forecast_store_code_scope_sprint4)
and dept_code in ('11', '12', '14')
),

-- Add real item_id to this table
item_sub_date_mapping as (
select
    item_id,
    sub_id,
    date_key
-- from fds.p4cm_item_map
from {database}.p4cm_item_map_complete 
where date_key >= cast({starting_date} as string)
and date_key < cast({ending_date} as string)
and item_id in (select distinct item_id from {database}.forecast_itemid_list_threebrands_sprint4)
group by item_id, sub_id, date_key
)

select
    a.*,
    b.item_id
from forecast_assortment a
left join item_sub_date_mapping b
on b.date_key = a.date_key
and b.sub_id = a.sub_id
;

-- invalidate metadata {database}.forecast_assortment_full;
