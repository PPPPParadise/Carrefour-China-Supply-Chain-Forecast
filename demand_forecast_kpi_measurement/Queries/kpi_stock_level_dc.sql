/* =======================================================
                      Module KPI Monitor
                        Stock level - DC
==========================================================
*/

/*
Input:
    * lfms.daily_dcstock
    * vartefact.forecast_itemid_list_threebrands_sprint4
    * vartefact.forecast_dm_dc_orders
    * vartefact.forecast_dc_item_details
To:
    * vartefact.monitor_stock_level_dc
*/

create table {database_name}.monitor_stock_level_dc as

with item_flag as
(
    select dept_code, item_code, sub_code, 1 as in_dm
    from vartefact.forecast_dm_dc_orders
    where run_date between "{date_start}" and "{date_end}"
    group by dept_code, item_code, sub_code
),

-- 
item_dc_stock as
(
    select
        a.item_id, a.sub_id, a.dc_site, a.date_key, c.rotation, a.stock_available_sku,
        nvl(b.in_dm, 0) as in_dm
    from
        lfms.daily_dcstock a
        left join item_flag b
        on
            a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
        inner join vartefact.forecast_dc_item_details c
        on
            concat(c.dept_code, c.item_code, c.sub_code) = a.item_code
    where
        a.dc_site = 'DC1'  -- only keep the DC in scope
        and a.item_id in (
            select item_id
            from vartefact.forecast_itemid_list_threebrands_sprint4
        )
        and a.date_key between "{date_start}" and "{date_end}"
)

-- sum of all available_sku of all items, group by DM/non-DM
select
    dc_site, in_dm, upper(rotation) as rotation, date_key, sum(stock_available_sku) as stock_level
from
    item_dc_stock
group by
    dc_site, in_dm, upper(rotation), date_key
