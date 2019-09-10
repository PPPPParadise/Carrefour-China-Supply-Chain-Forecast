with item_flag as
(
    select dept_code, item_code, sub_code, 1 as in_dm
    from vartefact.forecast_dm_dc_orders
    where first_order_date between "{date_start}" and "{date_end}"
    group by dept_code, item_code, sub_code
),
dc_stock_in_scope as
(
    select
        ldd.date_key, ldd.dc_site, dc.rotation, dc.holding_code, ldd.stock_available_sku,
        nvl(b.in_dm, 0) as in_dm,
        case when stock_available_sku > 0 then 0 else 1 end  as oos_flag
    from
        lfms.daily_dcstock ldd
        join vartefact.v_forecast_inscope_dc_item_details dc
        on
            concat(dc.dept_code, dc.item_code, dc.sub_code) = ldd.item_code
            and dc.rotation <> 'X' 
        left join item_flag b
        on
            concat(b.dept_code, b.item_code, b.sub_code) = ldd.item_code
    where
        ldd.dc_site = 'DC1'
        and ldd.warehouse_code='KS01'
        and ldd.date_key between "{date_start}" and "{date_end}"
)
insert overwrite table vartefact.foreacst_dc_monitor 
partition (run_date)
select
    dc_site, rotation, holding_code, in_dm, date_key,
    cast(count(1) as int) as n_items, cast(sum(1-oos_flag) as int) as n_not_oos_items,
    cast(sum(stock_available_sku) as DECIMAL(15,4)) as total_stock,
    "{date_end}" as run_date
from
    dc_stock_in_scope
group by
    dc_site, rotation, holding_code, in_dm, date_key