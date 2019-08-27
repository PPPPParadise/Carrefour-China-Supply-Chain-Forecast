/* =======================================================
                      Module KPI Monitor
                        Stock level - Store
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * fds.p4cm_daily_stock
    * ods.nsa_dm_theme
    * vartefact.forecast_sprint4_add_dm_to_daily
    * vartefact.forecast_itemid_list_threebrands_sprint4
    * vartefact.forecast_store_code_scope_sprint4
To:
    * vartefact.monitor_stock_level_store
*/

create table if exists {database_name}.monitor_stock_level_store;

create table {database_name}.monitor_stock_level_store as

-- add dm flag, groupby item, flag if item going to appear in dm
-- item_flag as
-- (
--     select
--         a.item_id, a.sub_id,
--         -- in_future_dm = 1 as long as there is a future_dm within 70 days
--         case
--             when
--                 sum(case when nvl(a.days_to_next_dm, 99) <= 70 then 1 else 0 end) > 0
--             then 1 else 0 end  as in_future_dm
--     from item_dm_trxn a
--     group by
--         a.item_id, a.sub_id
-- ),

with item_flag as
(
    select dept_code, item_code, sub_code, 1 as in_dm
    from vartefact.forecast_dm_orders
    where run_date between "{date_start}" and "{date_end}"
    group by dept_code, item_code, sub_code
),

-- 
item_store_stock as
(
    select
        a.item_id, a.sub_id, a.store_code, a.date_key, c.rotation, a.balance_qty,
        nvl(b.in_dm, 0) as in_dm
    from
        fds.p4cm_daily_stock a
        left join item_flag b
        on
            a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
        inner join vartefact.forecast_item_details c
        on
            a.dept_code = c.dept_code
            and a.item_code = c.item_code
            and a.sub_code = c.sub_code
    where
        a.date_key between "{date_start}" and "{date_end}"
)

-- sum of all available_sku of all items, group by DM/non-DM
select
    store_code, in_dm, upper(rotation) as rotation, date_key, sum(balance_qty) as stock_level
from
    item_store_stock
group by
    store_code, upper(rotation), in_dm, date_key
