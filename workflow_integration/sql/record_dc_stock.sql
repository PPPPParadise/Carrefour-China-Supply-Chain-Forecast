with dc_stock as (
    select 
        from_timestamp(date_add(to_timestamp(date_key, 'yyyyMMdd'), -1), 'yyyyMMdd') as date_key,
        dc_site,
        stock_available_sku,
        item_code,
        lpp
    from lfms.daily_dcstock 
    where
        dc_site = 'DC1'
        and warehouse_code='KS01'
),
item_flag as
(
    select dept_code, item_code, sub_code, 1 as in_dm
    from vartefact.forecast_dm_dc_orders
    where first_delivery_date between "{date_start}" and "{date_end}"
    group by dept_code, item_code, sub_code
),
dc_stock_in_scope as
(
    select
        ldd.date_key, ldd.dc_site, dc.rotation, dc.holding_code, ldd.stock_available_sku,
        ldd.lpp, dc.primary_ds_supplier as ds_supplier_code,
        nvl(b.in_dm, 0) as in_dm,
        case when stock_available_sku > 0 then 0 else 1 end  as oos_flag
    from
        dc_stock ldd
        join vartefact.v_forecast_inscope_dc_item_details dc
        on
            concat(dc.dept_code, dc.item_code, dc.sub_code) = ldd.item_code
            and dc.rotation <> 'X' 
        left join item_flag b
        on
            concat(b.dept_code, b.item_code, b.sub_code) = ldd.item_code
    where
        ldd.date_key between "{date_start}" and "{date_end}"
)
insert overwrite table vartefact.foreacst_dc_monitor 
partition (run_date)
select
    dc_site, rotation, holding_code, ds_supplier_code, in_dm, date_key,
    cast(count(1) as int) as n_items, cast(sum(1-oos_flag) as int) as n_not_oos_items,
    cast(sum(stock_available_sku) as DECIMAL(15,4)) as total_stock,
    cast(sum(stock_available_sku * lpp) as DECIMAL(15,4)) as total_stock_value,                                 
    "{date_end}" as run_date
from
    dc_stock_in_scope
group by
    dc_site, rotation, holding_code, ds_supplier_code, in_dm, date_key