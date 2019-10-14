/* =======================================================
                      Module KPI Monitor
                        Consistency
==========================================================
*/
select
    dc.rotation,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.rotation,
    dc.primary_ds_supplier,
    dc.qty_per_box,
    ceil(
        sum(cast(order_qty_in_pieces as int)) / cast (dc.qty_per_box as int)
    ) as order_qty
from
    vartefact.forecast_store_daily_order_files ord
    join vartefact.v_forecast_inscope_dc_item_details dc on ord.dept_code = dc.dept_code
    and ord.item_code = dc.item_code
    and ord.sub_code = dc.sub_code
where
    ord.date_key >= '{END_MONDAY}'
    and ord.date_key <= '{CONSISTENCY_END}'
    and dc.rotation = 'X'
group by
    dc.rotation,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.rotation,
    dc.primary_ds_supplier,
    dc.qty_per_box
            
union
            
select
    dc.rotation,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.rotation,
    dc.primary_ds_supplier,
    dc.qty_per_box,
    ceil(
        sum(cast(ord.order_in_pieces as int)) / cast (dc.qty_per_box as int)
    ) as order_qty
from
    vartefact.forecast_dc_daily_order_files ord
    join vartefact.v_forecast_inscope_dc_item_details dc on concat(dc.dept_code, dc.item_code, dc.sub_code) = ord.item_code
where
    date_key >= '{END_MONDAY}'
    and date_key <= '{CONSISTENCY_END}'
group by
    dc.rotation,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.rotation,
    dc.primary_ds_supplier,
    dc.qty_per_box