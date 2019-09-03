/* =======================================================
                      Module KPI Monitor
                        Detention rate
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * {database_name}.forecast_dc_orders_hist
    * {database_name}.forecast_dc_item_details
    * {database_name}.forecast_xdock_orders_hist
    * {database_name}.forecast_store_item_details   
To:
    * vartefact.monitor_detention_rate_store
*/

create table if exists {database_name}.monitor_consistency_order_hist;

create table {database_name}.monitor_consistency_order_hist as

with dc_AB as (
select
    a.sub_id, a.item_id, concat(a.dept_code, a.item_code, a.sub_code) as item_code, a.con_holding,
    a.order_day, a.run_date, a.order_qty,
    -- b.rotation, b.pcb, b.local_name as item_name, b.holding_name as supplier_name
    b.rotation, b.qty_per_box as pcb, b.primary_barcode as barcode,
    b.primary_ds_supplier_name as supplier_name, b.item_name_local as item_name
from
    {database_name}.forecast_dc_orders_hist a
    -- left join vartefact.forecast_item_details b
    left join {database_name}.forecast_dc_item_details b
    on a.dept_code = b.dept_code and a.item_code = b.item_code and a.sub_code = b.sub_code
where
    a.run_date >= "{CONSISTENCY_START}"
    and a.order_day <= "{CONSISTENCY_END}"
    and b.dc_status = 'Active'
    and upper(b.rotation) in ('A', 'B')
), 

store_X as (
select
    a.sub_id, a.item_id, concat(a.dept_code, a.item_code, a.sub_code) as item_code, 
    a.con_holding,
    a.order_day, a.run_date, a.order_qty,
    -- b.rotation, b.pcb, b.local_name as item_name, b.holding_name as supplier_name
    b.rotation, b.qty_per_unit as pcb, 'Missing' as barcode,
    b.ds_supplier_code as supplier_name, b.cn_name as item_name
from
    {database_name}.forecast_xdock_orders_hist  a 
    -- left join vartefact.forecast_item_details b
    left join {database_name}.forecast_store_item_details b
    on a.dept_code = b.dept_code 
    and a.item_code = b.item_code 
    and a.sub_code = b.sub_code
    and a.store_code = b.store_code
where
    a.run_date >= "{CONSISTENCY_START}"
    and a.order_day <= "{CONSISTENCY_END}"
    and b.store_status = 'Active'
    and upper(b.rotation) = 'X'
)
select *
from dc_AB
union 
select *
from store_X 
