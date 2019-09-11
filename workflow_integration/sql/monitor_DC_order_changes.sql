/*
DC level: Xdocking items excluded 

Input: {database_name}.forecast_dc_daily_order_files
       {database_name}.v_forecast_inscope_dc_item_details
       lfms.ord 
*/  
with 
    time_period as (
        select 
            distinct 
            date_key
        from {database_name}.forecast_dc_daily_order_files
        where date_key between '{date_start}' and '{date_end}'
    ),
    item_list as (
        select 
            dept_code, item_code, sub_code, qty_per_box, rotation, 
            holding_code, risk_item_unilever
        from {database_name}.v_forecast_inscope_dc_item_details
        where upper(rotation) != 'X'
    ),
    complete_item as (
        select 
            tp.date_key,
            il.dept_code,
            il.item_code, 
            il.sub_code,
            il.rotation,
            il.holding_code, 
            il.risk_item_unilever,
            il.qty_per_box 
        from time_period tp
        left join item_list il
        on 1=1
    ), 
    order_qty_model as (
        select 
            a.dept_code,
            a.item_code,
            a.sub_code,
            a.qty_per_box,
            a.holding_code,
            a.risk_item_unilever,
            a.rotation,
            to_date(to_timestamp(a.date_key, 'yyyyMMdd')) as date_key,
            b.supplier_code,
            b.warehouse,
            b.delivery_date,
            b.item_subcode_name_local, 
            if(b.purchase_quantity is null, 0, cast(b.purchase_quantity as int)) as purchase_quantity, 
            b.main_barcode,
            b.service_level,
            if(b.purchase_quantity is null, 0, cast(b.purchase_quantity as int)) * a.qty_per_box as order_qty_model 
        from complete_item a 
        left join {database_name}.forecast_dc_daily_order_files b 
        on concat(a.dept_code, a.item_code, a.sub_code) = b.item_code
        and a.date_key = b.date_key
    ), 
    carr_dc_order as (
        select 
            department_code, 
            item_code, 
            sub_code, 
            sum(basic_order_qty) as basic_order_qty,
            count(1) as n_actual_orders, 
            to_date(order_date) as order_date 
        from lfms.ord 
        where 
            date_format(order_date, 'yyyyMMdd') between '{date_start}' and '{date_end}'
            and dc_site = 'DC1'
            and mother_warehouse_code in ('KS01', 'KX01')
        group by department_code, item_code, sub_code, to_date(order_date)
    )
select 
    ord_m.*, 
    if(ord_c.basic_order_qty is null, 0, ord_c.basic_order_qty) as basic_order_qty,
    if(ord_c.n_actual_orders is null, 0, ord_c.n_actual_orders) as n_actual_orders,
    if(ord_c.basic_order_qty is null, 0, ord_c.basic_order_qty) - ord_m.order_qty_model as order_diff

from order_qty_model ord_m 
left join carr_dc_order ord_c 
on  ord_m.dept_code = ord_c.department_code
and ord_m.item_code = ord_c.item_code
and ord_m.sub_code = ord_c.sub_code 
and ord_m.date_key = ord_c.order_date

