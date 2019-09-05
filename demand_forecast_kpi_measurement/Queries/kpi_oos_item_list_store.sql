drop table if exists {database_name}.monitor_oos_item_list_store;

create table {database_name}.monitor_oos_item_list_store as

with flow_type as (
    select dept_code, item_code, sub_code, upper(rotation) as rotation
    from {database_name}.forecast_store_item_details 
    where store_status = 'Active' 
    group by dept_code, item_code, sub_code, rotation
),
store_stock_in_scope as
(
    select
        a.store_code, 
        a.date_key, 
        a.dept_code,
        a.item_code,
        a.sub_code,
        a.balance_qty,
        b.rotation
    
    from
        fds.p4cm_daily_stock a
        
    inner join flow_type b
        on a.dept_code = b.dept_code
        and a.item_code = b.item_code
        and a.sub_code = b.sub_code
    
    where
        a.date_key = "{oos_check_date}" 
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4)
        -- store_code in scope
        and a.store_code in ( 
            select stostocd 
            from {database_name}.forecast_store_code_scope_sprint4)
        and a.store_code like '1%'
)
select *
from store_stock_in_scope 
where balance_qty <= 0 
