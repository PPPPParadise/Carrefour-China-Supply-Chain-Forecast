/* ================================================================
                              Module 3
                            BP dataflow
    Step 6: identify groups with too high error
===================================================================
*/

-- Input: {database}.grouped_to_be_shipment_groupped
-- Output: {database}.forecast_item_store_perc_flagged
create table {database}.forecast_item_store_perc_flagged as
with distinct_selected as
-- drop dupplicates in link table
(select distinct
    item_id,
    sub_id,
    store_code,
    group_id,
    sales_qty,
    delivery_date,
    delivery_qty_sum
from {database}.grouped_to_be_shipment_groupped),     

ratio_per_item_store_group as
(select
    item_id,
    sub_id,
    store_code,
    group_id,
    sum(sales_qty) as sales_qty,
    max(delivery_qty_sum) as delivery_qty_sum,
    sum(sales_qty) / max(delivery_qty_sum) as ratio
from distinct_selected
group by item_id, sub_id,store_code, group_id),

ratio_per_item as
(select
    item_id,
    sub_id,
    store_code,
    sum(sales_qty) as sales_qty_flagged,
    sum(delivery_qty_sum) as delivery_qty_sum,
    sum(sales_qty) / sum(delivery_qty_sum) * 100 as perc_flagged
from ratio_per_item_store_group
group by item_id, sub_id, store_code)

select *
from ratio_per_item
order by perc_flagged desc
;