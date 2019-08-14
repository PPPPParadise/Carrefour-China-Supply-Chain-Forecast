/* =======================================================
                      Module KPI Monitor
                        Stock level - DC
==========================================================
*/

/*
Input:
    * lfms.daily_dcstock
    * ods.nsa_dm_theme
    * vartefact.forecast_sprint4_add_dm_to_daily
    * vartefact.forecast_itemid_list_threebrands_sprint4
    * vartefact.forecast_store_code_scope_sprint4
To:
    * vartefact.monitor_stock_level_dc
*/

create table {database_name}.monitor_stock_level_dc as

-- filter the items in scope and calculate days till next dm
with item_dm_trxn as
(
    select
        a.item_id, a.sub_id, a.store_code, a.next_dm_theme_id, a.date_key,
        b.theme_start_date,
        datediff(to_timestamp(b.theme_start_date, 'yyyy-MM-dd'),
                 to_timestamp(a.date_key, 'yyyyMMdd')
        ) as days_to_next_dm
        -- use row number to drop duplicates by subset item-store-date
        -- row_number() over (partition by a.item_id, a.sub_id, a.store_code, a.date_key
        --                    order by a.item_id, a.sub_id, a.store_code, a.date_key) as row_count
    from
        {database_name}.forecast_sprint4_add_dm_to_daily a
        left join
            ods.nsa_dm_theme b
        on
            a.next_dm_theme_id = b.dm_theme_id
    where
        -- date range
        a.date_key between "{date_start}" and "{date_end}"
        -- item_id in scope
        and a.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4
        )
        -- store_code in scope
        and a.store_code in (
            select stostocd
            from {database_name}.forecast_store_code_scope_sprint4
        )
        and a.store_code like '1%'
),

-- item_dm as
-- (
--     select * from item_dm_trxn where row_count = 1
-- ),

-- add dm flag, groupby item, flag if item going to appear in dm in any store
item_flag as
(
    select
        a.item_id, a.sub_id,
        -- in_future_dm = 1 if next_dm is within 10 weeks
        case when
            sum(case when nvl(a.days_to_next_dm, 99) <= 70 then 1 else 0 end) > 0
            then 1 else 0 end as in_future_dm
    from item_dm_trxn a
    group by
        a.item_id, a.sub_id
),

-- 
item_dc_stock as
(
    select
        a.item_id, a.sub_id, a.dc_site, a.date_key, c.rotation, a.stock_available_sku,
        case when b.in_future_dm > 0 then 1 else 0 end  as in_future_dm
    from
        lfms.daily_dcstock a
        inner join item_flag b
        on
            a.item_id = b.item_id
            and a.sub_id = b.sub_id
            -- and a.date_key = b.date_key
        left join vartefact.forecast_item_details c
        on
            concat(c.dept_code, c.item_code, c.sub_code) = a.item_code
    where
        a.dc_site = 'DC1'  -- only keep the DC in scope
        and a.date_key between "{date_start}" and "{date_end}"
)

-- sum of all available_sku of all items, group by DM/non-DM
select
    dc_site, in_future_dm, upper(rotation) as rotation, date_key, sum(stock_available_sku) as stock_level
from
    item_dc_stock
group by
    dc_site, in_future_dm, upper(rotation), date_key
