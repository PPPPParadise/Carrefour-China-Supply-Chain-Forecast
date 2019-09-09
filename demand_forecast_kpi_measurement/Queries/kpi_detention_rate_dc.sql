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
    * lfms.daily_dcstock
    * vartefact.forecast_itemid_list_threebrands_sprint4
    * vartefact.forecast_dc_item_details   -- to get item rotation
To:
    * vartefact.monitor_detention_rate_dc
*/


drop table if exists {database_name}.monitor_detention_rate_dc;

create table {database_name}.monitor_detention_rate_dc as

with flow_type as
(
    select
        concat(dept_code, item_code, sub_code) as item_code_long,
        rotation
    from 
        {database_name}.v_forecast_inscope_dc_item_details
    group by concat(dept_code, item_code, sub_code), rotation

),

dcstock_in_scope as
(
    select
        a.dc_site,
        a.date_key,
        b.rotation,
        case when a.stock_available_sku > 0 then 0 else 1 end  as oos_flag
    from lfms.daily_dcstock a
    inner join flow_type b  -- this table is manually loaded
    on
        a.item_code = b.item_code_long
    where
        a.date_key between "{date_start}" and "{date_end}"
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4
        )
        and a.warehouse_code = 'KS01'  -- only care about the onstock type
        and a.dc_site = 'DC1'
        and lower( b.rotation ) <> 'x'  -- exclude xdocking
)

select
    dc_site, date_key, rotation,
    count(1) as n_items, sum(1-oos_flag) as n_not_oos_items,
    sum(1-oos_flag) / count(1) as detention_rate
from
    dcstock_in_scope
group by
    dc_site, date_key, rotation
order by
    date_key, dc_site, rotation
