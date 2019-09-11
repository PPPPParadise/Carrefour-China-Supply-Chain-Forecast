with dcstock_in_scope as
(
    select
        a.dc_site,
        a.warehouse_code,
        a.date_key,
        a.stock_available_sku,
        b.rotation,
        b.holding_code,
        b.primary_ds_supplier,
        b.dept_code,
        b.item_code,
        b.sub_code,
        b.item_name_local,
        a.item_id,
        a.sub_id
    from lfms.daily_dcstock a
    inner join {database_name}.v_forecast_inscope_dc_item_details b  -- this table is manually loaded
    on
        a.item_code = concat(b.dept_code, b.item_code, b.sub_code)
    where
        a.date_key = date_format(date_add(to_timestamp("{oos_check_date}", 'yyyyMMdd'), 1), 'yyyyMMdd')
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4
        )
        and a.warehouse_code = 'KS01'  -- only care about the onstock type
        and a.dc_site = 'DC1'
        and upper( b.rotation ) <> 'X'  -- exclude xdocking
)

select * 
from dcstock_in_scope 
where stock_available_sku <= 0
order by rotation, holding_code, item_code
