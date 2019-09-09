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
    * fds.p4cm_daily_stock
    * vartefact.forecast_store_code_scope_sprint4
    * vartefact.forecast_itemid_list_threebrands_sprint4
    * vartefact.forecast_item_details   -- to get item flow type
To:
    * vartefact.monitor_detention_rate_store
*/


drop table if exists {database_name}.monitor_detention_rate_store;

create table {database_name}.monitor_detention_rate_store as

with flow_type as (
    select dept_code, item_code, sub_code, upper(rotation) as rotation
    from {database_name}.v_forecast_inscope_store_item_details
    group by dept_code, item_code, sub_code, rotation
),

store_stock_in_scope as
(
    select
        a.store_code, a.date_key, b.rotation,
        case when balance_qty > 0 then 0 else 1 end  as oos_flag
    from
        fds.p4cm_daily_stock a
        inner join flow_type b
        on
            a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
    where
        a.date_key between "{date_start}" and "{date_end}"
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4
        )
        -- store_code in scope
        and (
            a.store_code in (
                select stostocd from {database_name}.forecast_store_code_scope_sprint4
            )
            and a.store_code like '1%'
        )
        -- exclude xdocking
        -- and lower( b.rotation ) <> 'x'
)

select
    store_code, date_key, rotation,
    count(1) as n_items, sum(1-oos_flag) as n_not_oos_items,
    sum(1-oos_flag) / count(1) as detention_rate
from
    store_stock_in_scope
group by
    store_code, date_key, rotation
order by
    date_key, store_code, rotation
