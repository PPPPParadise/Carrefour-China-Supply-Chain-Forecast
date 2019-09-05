drop table if exists {database_name}.monitor_oos_item_list_dc;

create table {database_name}.monitor_oos_item_list_dc as

with flow_type as
(
    select
        concat(dept_code, item_code, sub_code) as item_code_long,
        rotation
    from 
        {database_name}.forecast_dc_item_details
    where dc_status = 'Active'
    group by concat(dept_code, item_code, sub_code), rotation

),
dcstock_in_scope as
(
    select
        a.dc_site,
        a.date_key,
        a.item_code, 
        a.stock_available_sku,
        b.rotation
    
    from lfms.daily_dcstock a
    inner join flow_type b  -- this table is manually loaded
    on
        a.item_code = b.item_code_long
    where
        a.date_key = "{oos_check_date}"
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4)
    
        and a.warehouse_code = 'KS01'  -- only care about the onstock type
        and a.dc_site = 'DC1'
        and lower( b.rotation ) <> 'x'  -- exclude xdocking
)

select * 
from dcstock_in_scope 
where stock_available_sku <= 0

