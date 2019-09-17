with item_flag as
(
    select dept_code, item_code, sub_code, 1 as in_dm
    from vartefact.forecast_dm_orders
    where first_delivery_date between "{date_start}" and "{date_end}"
    group by dept_code, item_code, sub_code
),
store_stock_in_scope as
(
    select
        fpds.date_key, fpds.store_code, id.rotation, id.con_holding, fpds.balance_qty,
        fpds.lpp, id.ds_supplier_code,
        nvl(b.in_dm, 0) as in_dm,
        case when balance_qty > 0 then 0 else 1 end  as oos_flag
    from
        fds.p4cm_daily_stock fpds
        join vartefact.v_forecast_inscope_store_item_details id
        on
            fpds.dept_code = id.dept_code
            and fpds.item_code = id.item_code
            and fpds.sub_code = id.sub_code
            and fpds.store_code = id.store_code
        left join item_flag b
        on
            fpds.dept_code = b.dept_code
            and fpds.item_code = b.item_code
            and fpds.sub_code = b.sub_code
    where
        fpds.date_key between "{date_start}" and "{date_end}"
        and id.dc_status != 'Stop'
)
insert overwrite table vartefact.foreacst_store_monitor 
partition (run_date)
select
    store_code, rotation, con_holding, ds_supplier_code, in_dm, date_key,
    cast(count(1) as int) as n_items, cast(sum(1-oos_flag) as int) as n_not_oos_items,
    cast(sum(balance_qty) as DECIMAL(15,4)) as total_stock,
    cast(sum(balance_qty * lpp) as DECIMAL(15,4)) as total_stock_value,         
    "{date_end}" as run_date
from
    store_stock_in_scope
group by
    store_code, rotation, con_holding, ds_supplier_code, in_dm, date_key