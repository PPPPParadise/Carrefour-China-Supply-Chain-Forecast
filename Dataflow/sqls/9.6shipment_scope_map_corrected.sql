/* =====================================================
                       Module 3
                     BP dataflow
    Step 4: Create a shipment table within our scope
========================================================
*/

-- Input:lfms.daily_shipment
--       {database}.p4cm_item_map_complete
--       {database}.forecast_store_code_scope_sprint4
--       {database}.forecast_itemid_list_threebrands_sprint4 
-- Output: {database}.shipment_scope_map_corrected
-- SCALA script to link shipments and baskets
-- Usage: create a list of shipments to be linked to baskets by SCALA script

create table {database}.shipment_scope_map_corrected as
with shipment_item_id as (
-- shipment with bp in east region
select
    a.delivery_date,
    a.delivery_qty_in_sku,
    a.store_code,
    b.item_id,
    b.sub_id
from (
    select
        *
    from lfms.daily_shipment
    where substring(store_order_no,3,4) = 'KPM1' -- Planned BP 
        and date_key >= cast({starting_date} as string)
        and date_key < cast({ending_date} as string)
    ) as a
left join (
    select
        max(item_id) as item_id,
        max(sub_id) as sub_id,
        item_code,
        sub_code,
        date_key
    from {database}.p4cm_item_map_complete
    group by item_code, sub_code, date_key) as b
    on a.item_code = b.item_code
        and a.sub_code = b.sub_code
        and a.date_key = b.date_key
)

select
    item_id,
    sub_id,
    store_code,
    delivery_date,
    sum(delivery_qty_in_sku) as delivery_qty_sum
from shipment_item_id
where store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
    and item_id in (select item_id from {database}.forecast_itemid_list_threebrands_sprint4)
group by item_id, sub_id, store_code, delivery_date;
-- 5112 rows
