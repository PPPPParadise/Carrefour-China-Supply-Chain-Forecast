/* =======================================================
                      Module KPI Monitor
                        Service level- Store level
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * fds.p4cm_daily_order 
    * lfms.daily_shipment 
    * {database}.forecast_store_code_scope_sprint4 
    * {database}.forecast_itemid_list_threebrands_sprint4   -- items in scope
To:
    * {database}.monitor_service_level_item_dc
*/

drop table if exists {database_name}.monitor_service_level_item_store;

create table {database_name}.monitor_service_level_item_store as

with order_qty_tb as (
SELECT 
    date_key,
    order_date, 
    store_code,
    item_id,
    sub_id,
    item_code,
    sub_code,
    dept_code,
    supplier_code,
    order_number,
    order_qty, 
    ord_unit_qty, 
    qty_per_pack,
    order_qty * ord_unit_qty as order_qty_in_sku,
    row_number() OVER (PARTITION BY store_code, item_code, sub_code, dept_code, supplier_code, order_number
                        ORDER BY date_key desc) as row_
    
FROM fds.p4cm_daily_order 
where order_status = 'C' 
and order_date between to_timestamp("{date_start}", 'yyyyMMdd') and to_timestamp("{date_end}", 'yyyyMMdd')
and store_code in (SELECT stostocd from {database}.forecast_store_code_scope_sprint4)
and item_id in (SELECT DISTINCT item_id from {database}.forecast_itemid_list_threebrands_sprint4)
), 

order_qty_clean as (
select 
    order_date, 
    store_code,
    item_id,
    sub_id,
    item_code,
    sub_code,
    dept_code,
    supplier_code,
    order_number,
    order_qty, 
    ord_unit_qty, 
    qty_per_pack,
    order_qty_in_sku
from order_qty_tb
where row_ = 1
group by 
    order_date, 
    store_code,
    item_id,
    sub_id,
    item_code,
    sub_code,
    dept_code,
    supplier_code,
    order_number,
    order_qty, 
    ord_unit_qty, 
    qty_per_pack,
    order_qty_in_sku
),

store_receive as (
SELECT 
    store_code,
    dept_code,
    item_code,
    sub_code,
    store_order_no, 
    substr(store_order_no, 3, 4) as supplier_code,
    substr(store_order_no, 7, 3) as order_number,
    sum(delivery_qty_in_sku) as delivery_qty_in_sku
    
FROM lfms.daily_shipment 
where dc_site = 'DC1'
and date_key BETWEEN "{date_start}" and "{date_end}"
and store_code in (SELECT stostocd from {database}.forecast_store_code_scope_sprint4)
and item_id in (SELECT DISTINCT item_id from {database}.forecast_itemid_list_threebrands_sprint4)
and rank = 1  
GROUP BY 
    store_code,
    dept_code,
    item_code,
    sub_code,
    store_order_no 
), 

order_receive as (
select 
    s_ord.order_date, 
    s_ord.store_code, 
    s_ord.item_id,
    s_ord.sub_id,
    s_ord.item_code,
    s_ord.sub_code,
    s_ord.dept_code,
    s_ord.supplier_code,
    s_ord.order_number,
    s_ord.order_qty, 
    s_ord.ord_unit_qty, 
    s_ord.order_qty_in_sku,
    s_rec.delivery_qty_in_sku,
    item_list.holding_code 

from order_qty_clean s_ord

left join store_receive s_rec
on s_rec.store_code = s_ord.store_code
and s_rec.dept_code = s_ord.dept_code
and s_rec.item_code = s_ord.item_code
and s_rec.sub_code = s_ord.sub_code
and s_rec.supplier_code = s_ord.supplier_code
and s_rec.order_number = s_ord.order_number 

left join {database}.forecast_itemid_list_threebrands_sprint4 item_list 
on item_list.item_id = s_ord.item_id 
), 

service_lvl as (
select
    item_code,
    sub_code,
    dept_code,
    holding_code,
    sum(order_qty_in_sku) as order_qty_in_sku_sum, 
    sum(delivery_qty_in_sku) as delivery_qty_in_sku_sum,
    count(1) as orders_count,
    -- calculate service level (+0.001 is to avoid error when 0/0)
    if(sum(delivery_qty_in_sku) / (sum(order_qty_in_sku) + 0.00001) is null, 0, 
        sum(delivery_qty_in_sku) / (sum(order_qty_in_sku) + 0.00001)) as service_level

from order_receive  
group by
    item_code,
    sub_code, 
    dept_code,
    holding_code
)

select * 
from service_lvl 
; 